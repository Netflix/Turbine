package com.netflix.turbine.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurator;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.pipeline.ssl.SSLEngineFactory;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.URI;

public class HttpClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientFactory.class);

    private static final String HTTPS = "https";

    public static HttpClient<ByteBuf, ServerSentEvent> createFromURI(URI uri) {
        HttpClientBuilder<ByteBuf, ServerSentEvent> builder = RxNetty.<ByteBuf, ServerSentEvent>newHttpClientBuilder(uri.getHost(), uri.getPort());
        final PipelineConfigurator<HttpClientResponse<ServerSentEvent>, HttpClientRequest<ByteBuf>> pipelineConfigurator;

        PipelineConfigurator<HttpClientResponse<ServerSentEvent>, HttpClientRequest<ByteBuf>> sse = PipelineConfigurators.<ByteBuf>sseClientConfigurator();

        if (HTTPS.equalsIgnoreCase(uri.getScheme())) {
            logger.debug("Applying SSLEngineFactory to stream https://{}:{}{}", uri.getHost(), uri.getPort(), uri.getPath());
//            builder = builder.withSslEngineFactory(new DefaultSSLEngineFactory());
            PipelineConfigurator<HttpClientResponse<ServerSentEvent>, HttpClientRequest<ByteBuf>> ssl = PipelineConfigurators.sslConfigurator(new DefaultSSLEngineFactory());
            pipelineConfigurator = PipelineConfigurators.composeConfigurators(ssl, sse);
        } else {
            pipelineConfigurator = sse;
        }

        builder.pipelineConfigurator(pipelineConfigurator);

        return builder.build();
    }

    private static class DefaultSSLEngineFactory implements SSLEngineFactory {

        private final SslContext sslCtx;

        private DefaultSSLEngineFactory() {
            try {
                SslProvider sslProvider = SslContext.defaultClientProvider();
                sslCtx = SslContext.newClientContext(sslProvider);
            } catch (SSLException e) {
                throw new IllegalStateException("Failed to create default SSL context", e);
            }
        }

        @Override
        public SSLEngine createSSLEngine(ByteBufAllocator allocator) {
            return sslCtx.newEngine(allocator);
        }
    }
}
