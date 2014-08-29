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
package com.netflix.turbine.internal;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import com.netflix.turbine.aggregator.AggregateString;

public class AggregateStringSerializer extends JsonSerializer<AggregateString> {

    @Override
    public void serialize(AggregateString as, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        Map<String, Integer> values = as.values();
        if (values.size() == 1) {
            String k = values.entrySet().iterator().next().getKey();
            if ("false".equals(k)) {
                jgen.writeBoolean(false);
            } else if ("true".equals(k)) {
                jgen.writeBoolean(true);
            } else {
                jgen.writeString(k);
            }
        } else {
            jgen.writeObject(values);
        }
    }

}
