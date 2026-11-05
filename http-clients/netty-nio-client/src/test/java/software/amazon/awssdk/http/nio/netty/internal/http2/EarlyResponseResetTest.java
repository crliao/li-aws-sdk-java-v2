/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.http.nio.netty.internal.http2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.Flowable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

/**
 * A server may send a complete response and then RST_STREAM(NO_ERROR) before the client has finished sending its
 * request (RFC 9113 section 8.1). The client must not discard the complete response in that case.
 */
public class EarlyResponseResetTest {
    private static final int ITERATIONS = 200;

    private final List<Boolean> requestHeadersEndStream = new CopyOnWriteArrayList<>();
    private NioEventLoopGroup serverGroup;
    private ServerSocketChannel serverChannel;
    private SdkAsyncHttpClient netty;
    private volatile boolean resetAfterResponse = true;
    private volatile ChannelHandlerContext lastServerContext;
    private volatile Http2HeadersFrame lastRequestHeaders;
    private final CountDownLatch serverResponded = new CountDownLatch(1);

    @BeforeEach
    public void setup() throws InterruptedException {
        serverGroup = new NioEventLoopGroup();
        serverChannel = (ServerSocketChannel) new ServerBootstrap()
            .channel(NioServerSocketChannel.class)
            .group(serverGroup)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    Http2FrameCodec codec = Http2FrameCodecBuilder.forServer()
                                                                  .autoAckPingFrame(true)
                                                                  .decoderEnforceMaxRstFramesPerWindow(0, 0)
                                                                  .build();
                    ch.pipeline().addLast(codec);
                    ch.pipeline().addLast(new RespondThenResetHandler());
                }
            })
            .localAddress(0)
            .bind().await().channel();

        netty = NettyNioAsyncHttpClient.builder()
                                       .protocol(Protocol.HTTP2)
                                       .maxConcurrency(1)
                                       .build();
    }

    @AfterEach
    public void teardown() throws InterruptedException {
        netty.close();
        serverChannel.close().await();
        serverGroup.shutdownGracefully().await();
    }

    @Test
    public void emptyBodyRequest_sendsEndStreamOnHeadersAndSucceeds() {
        for (int i = 0; i < ITERATIONS; i++) {
            execute(publisher(Optional.of(0L), Flowable.just(ByteBuffer.allocate(0))), new RecordingHandler()).join();
        }
        assertThat(requestHeadersEndStream).hasSize(ITERATIONS).containsOnly(true);
    }

    @Test
    public void emptyBodyRequest_subscriptionDependentBody_isSubscribedAndSucceeds() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        // Like AsyncRequestBody.forBlockingInputStream(0L): the body is only produced after it has been subscribed to.
        Publisher<ByteBuffer> body = s -> s.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                subscribed.countDown();
                CompletableFuture.runAsync(s::onComplete);
            }

            @Override
            public void cancel() {
            }
        });
        RecordingHandler handler = new RecordingHandler();

        execute(publisher(Optional.of(0L), body), handler).get(5, TimeUnit.SECONDS);

        assertThat(subscribed.getCount()).isZero();
        assertThat(requestHeadersEndStream).containsExactly(true);
        assertThat(handler.errors).isEmpty();
    }

    @Test
    public void emptyBodyRequest_publisherError_failsRequest() {
        IllegalStateException failure = new IllegalStateException("boom");
        RecordingHandler handler = new RecordingHandler();

        CompletableFuture<Void> result = execute(publisher(Optional.of(0L), Flowable.error(failure)), handler);

        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasRootCause(failure);
        assertThat(handler.errors).hasSize(1);
        assertThat(requestHeadersEndStream).isEmpty();
    }

    @Test
    public void streamedRequest_writeFailsAfterResponseCompleted_succeeds() throws Exception {
        resetAfterResponse = false;
        ControlledPublisher body = new ControlledPublisher();
        RecordingHandler handler = new RecordingHandler();

        CompletableFuture<Void> result = execute(publisher(Optional.empty(), body), handler);

        // The server has sent a complete response while the request body (and so the request write promise) is
        // still pending.
        assertThat(serverResponded.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(requestHeadersEndStream).containsExactly(false);
        assertThat(result).isNotDone();

        // Resetting the stream delivers the complete response to the client and closes the stream.
        resetStream().sync();
        result.get(5, TimeUnit.SECONDS);
        assertThat(handler.responseComplete.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(body.cancelled.await(5, TimeUnit.SECONDS)).isTrue();

        // The request body completes only now, so the final request write (END_STREAM) fails on the closed stream
        // strictly after the response completed. That late failure must not be reported to the response handler.
        body.complete();
        assertThat(handler.errored.await(1, TimeUnit.SECONDS)).isFalse();
        assertThat(handler.errors).isEmpty();
        assertThat(result).isCompletedWithValue(null);
    }

    private CompletableFuture<Void> execute(SdkHttpContentPublisher body, SdkAsyncHttpResponseHandler handler) {
        SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                                                       .method(SdkHttpMethod.GET)
                                                       .protocol("http")
                                                       .host("localhost")
                                                       .port(serverChannel.localAddress().getPort())
                                                       .build();
        return netty.execute(AsyncExecuteRequest.builder()
                                                .request(request)
                                                .requestContentPublisher(body)
                                                .responseHandler(handler)
                                                .build());
    }

    private static SdkHttpContentPublisher publisher(Optional<Long> contentLength, Publisher<ByteBuffer> body) {
        return new SdkHttpContentPublisher() {
            @Override
            public Optional<Long> contentLength() {
                return contentLength;
            }

            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                body.subscribe(s);
            }
        };
    }

    private ChannelFuture resetStream() {
        Http2HeadersFrame request = lastRequestHeaders;
        return lastServerContext.writeAndFlush(new DefaultHttp2ResetFrame(Http2Error.NO_ERROR).stream(request.stream()));
    }

    private static final class RecordingHandler implements SdkAsyncHttpResponseHandler {
        private final List<Throwable> errors = new CopyOnWriteArrayList<>();
        private final CountDownLatch responseComplete = new CountDownLatch(1);
        private final CountDownLatch errored = new CountDownLatch(1);

        @Override
        public void onHeaders(SdkHttpResponse headers) {
        }

        @Override
        public void onStream(Publisher<ByteBuffer> stream) {
            Flowable.fromPublisher(stream).subscribe(b -> { }, errors::add, responseComplete::countDown);
        }

        @Override
        public void onError(Throwable error) {
            errors.add(error);
            errored.countDown();
        }
    }

    /**
     * A request body that stays open, keeping the request write pending, until {@link #complete()} is called.
     */
    private static final class ControlledPublisher implements Publisher<ByteBuffer> {
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private volatile Subscriber<? super ByteBuffer> subscriber;

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> s) {
            subscriber = s;
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                    cancelled.countDown();
                }
            });
        }

        void complete() {
            subscriber.onComplete();
        }
    }

    private class RespondThenResetHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof Http2HeadersFrame) {
                    Http2HeadersFrame requestHeaders = (Http2HeadersFrame) msg;
                    requestHeadersEndStream.add(requestHeaders.isEndStream());
                    lastServerContext = ctx;
                    lastRequestHeaders = requestHeaders;
                    ctx.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status("404"), true)
                                  .stream(requestHeaders.stream()));
                    if (resetAfterResponse && !requestHeaders.isEndStream()) {
                        ctx.write(new DefaultHttp2ResetFrame(Http2Error.NO_ERROR).stream(requestHeaders.stream()));
                    }
                    ctx.flush();
                    serverResponded.countDown();
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }
}
