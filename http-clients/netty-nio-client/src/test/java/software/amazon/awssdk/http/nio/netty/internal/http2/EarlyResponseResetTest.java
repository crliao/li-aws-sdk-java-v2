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

import io.netty.bootstrap.ServerBootstrap;
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
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
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
            execute(SdkHttpMethod.GET, Optional.of(0L));
        }
        assertThat(requestHeadersEndStream).hasSize(ITERATIONS).containsOnly(true);
    }

    @Test
    public void streamedRequest_resetAfterCompleteResponse_succeeds() {
        for (int i = 0; i < ITERATIONS; i++) {
            execute(SdkHttpMethod.GET, Optional.empty());
        }
        assertThat(requestHeadersEndStream).hasSize(ITERATIONS);
    }

    private void execute(SdkHttpMethod method, Optional<Long> contentLength) {
        SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                                                       .method(method)
                                                       .protocol("http")
                                                       .host("localhost")
                                                       .port(serverChannel.localAddress().getPort())
                                                       .build();
        Publisher<ByteBuffer> emptyBody = Flowable.just(ByteBuffer.allocate(0));

        AsyncExecuteRequest executeRequest =
            AsyncExecuteRequest.builder()
                               .request(request)
                               .requestContentPublisher(new SdkHttpContentPublisher() {
                                   @Override
                                   public Optional<Long> contentLength() {
                                       return contentLength;
                                   }

                                   @Override
                                   public void subscribe(Subscriber<? super ByteBuffer> s) {
                                       emptyBody.subscribe(s);
                                   }
                               })
                               .responseHandler(new SdkAsyncHttpResponseHandler() {
                                   @Override
                                   public void onHeaders(SdkHttpResponse headers) {
                                   }

                                   @Override
                                   public void onStream(Publisher<ByteBuffer> stream) {
                                       Flowable.fromPublisher(stream).forEach(b -> { });
                                   }

                                   @Override
                                   public void onError(Throwable error) {
                                   }
                               })
                               .build();

        netty.execute(executeRequest).join();
    }

    private class RespondThenResetHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof Http2HeadersFrame) {
                    Http2HeadersFrame requestHeaders = (Http2HeadersFrame) msg;
                    requestHeadersEndStream.add(requestHeaders.isEndStream());
                    ctx.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status("404"), true)
                                  .stream(requestHeaders.stream()));
                    if (!requestHeaders.isEndStream()) {
                        ctx.write(new DefaultHttp2ResetFrame(Http2Error.NO_ERROR).stream(requestHeaders.stream()));
                    }
                    ctx.flush();
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }
}
