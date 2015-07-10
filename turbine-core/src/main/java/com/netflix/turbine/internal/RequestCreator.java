/**
 * Copyright 2014 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.internal;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RequestCreator {

    private static final Logger logger = LoggerFactory.getLogger(RequestCreator.class);

    /**
     * Creates a {@link HttpClientRequest} for the supplied URI.
     * If user info is defined in the URI it'll be applied as basic authentication.
     *
     * @param uri The URI
     * @return The generated {@link HttpClientRequest} with basic auth (if applicable)
     */
    public static HttpClientRequest<ByteBuf> createRequest(URI uri) {
        String uriToUse = stripUserInfoFromUriIfDefined(uri).toASCIIString();
        HttpClientRequest<ByteBuf> request = HttpClientRequest.createGet(uriToUse);

        if (uri.getUserInfo() != null) {
            logger.debug("Adding basic authentication header for URI {}", uriToUse);
            String basicAuth = "Basic " + Base64.getEncoder().encodeToString(uri.getUserInfo().getBytes(UTF_8));
            request.withHeader("Authorization", basicAuth);
        }

        return request;
    }

    private static URI stripUserInfoFromUriIfDefined(URI uri) {
        final URI uriToUse;
        if (uri.getUserInfo() != null) {
            try {
                uriToUse = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else {
            uriToUse = uri;
        }
        return uriToUse;
    }
}
