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
package com.netflix.turbine.aggregator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AggregateStringTest {

    @Test
    public void testSingleValue() {
        AggregateString v = AggregateString.create("false", InstanceKey.create(1));
        assertEquals("{\"false\":1}", v.toJson());

        v = v.update("false", "false", InstanceKey.create(1));
        assertEquals("{\"false\":1}", v.toJson());

        v = v.update("false", "true", InstanceKey.create(1));
        assertEquals("{\"true\":1}", v.toJson());
    }

    @Test
    public void testMultipleValuesOnSingleInstance() {
        AggregateString v = AggregateString.create();
        InstanceKey instance = InstanceKey.create(1);
        v = v.update(null, "false", instance);
        assertEquals("{\"false\":1}", v.toJson());

        v = v.update("false", "false", instance);
        assertEquals("{\"false\":1}", v.toJson());

        v = v.update("false", "true", instance);
        assertEquals("{\"true\":1}", v.toJson());
    }

    @Test
    public void testMultipleValuesOnMultipleInstances() {
        AggregateString v = AggregateString.create();
        v = v.update(null, "false", InstanceKey.create(1));
        v = v.update(null, "true", InstanceKey.create(2));
        assertEquals("{\"false\":1,\"true\":1}", v.toJson());

        v = v.update("false", "false", InstanceKey.create(1));  // old value set so we'll decrement ... but same so no different
        assertEquals("{\"false\":1,\"true\":1}", v.toJson());
        assertEquals(2, v.instances().size());

        v = v.update(null, "false", InstanceKey.create(3));
        assertEquals("{\"false\":2,\"true\":1}", v.toJson());
        assertEquals(3, v.instances().size());

        v = v.update("false", "true", InstanceKey.create(1));
        assertEquals("{\"false\":1,\"true\":2}", v.toJson());

        v = v.update("false", "true", InstanceKey.create(3));
        assertEquals("{\"true\":3}", v.toJson());
        
        // remove an instance
        v = v.update("true", null, InstanceKey.create(1));
        assertEquals("{\"true\":2}", v.toJson());
        assertEquals(2, v.instances().size());
        
    }

}
