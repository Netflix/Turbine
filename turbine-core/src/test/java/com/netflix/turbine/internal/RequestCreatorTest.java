package com.netflix.turbine.internal;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import org.junit.Test;

import java.net.URI;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class RequestCreatorTest {

    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";

    @Test
    public void doesntAddAuthorizationHeaderWhenNoUserInfoIsDefinedInUri() throws Exception {
        // Given
        URI uri = new URI("http://myapp.com");

        // When
        HttpClientRequest<ByteBuf> request = RequestCreator.createRequest(uri);

        // Then
        assertFalse(request.getHeaders().contains(AUTHORIZATION_HEADER_NAME));
    }

    @Test
    public void addsAuthorizationHeaderWhenUserInfoIsDefinedInUri() throws Exception {
        // Given
        URI uri = new URI("http://username:password@myapp.com");

        // When
        HttpClientRequest<ByteBuf> request = RequestCreator.createRequest(uri);

        // Then
        assertEquals(basicAuthOf("username", "password"), request.getHeaders().getHeader(AUTHORIZATION_HEADER_NAME));
    }

    @Test
    public void removesUserInfoFromUriWhenUserInfoIsDefinedInUri() throws Exception {
        // Given
        URI uri = new URI("http://username:password@myapp.com");

        // When
        HttpClientRequest<ByteBuf> request = RequestCreator.createRequest(uri);

        // Then
        assertFalse(request.getUri().contains("username:password@"));
    }

    private String basicAuthOf(String username, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(UTF_8));
    }

}