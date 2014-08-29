/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine;

import com.netflix.turbine.internal.JsonUtility;

import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;

public class HttpServerDemo {

    public static void main(String args[]) {
        RxNetty.createHttpServer(8080, (request, response) -> {
            return Demo.getStream().flatMap(data -> {
                response.getHeaders().set("Content-Type", "text/event-stream");
                return response.writeAndFlush(new ServerSentEvent("1", "data", JsonUtility.mapToJson(data)));
            });
        }, PipelineConfigurators.sseServerConfigurator()).startAndWait();
    }
}
