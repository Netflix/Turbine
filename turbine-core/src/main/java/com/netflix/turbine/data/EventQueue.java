/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.data;

import com.netflix.turbine.handler.HandlerQueueTuple;

/**
 * Simple interface representing an event queue that can be used for dispatching events to when handing off 
 * data to other threads. See {@link HandlerQueueTuple} for details. 
 *
 * @param <E>
 */
public interface EventQueue<E> {

    /**
     * @return E
     */
    public E readEvent();
    
    /**
     * @param event
     * @return true/false indicating whether the write was successful
     */
    public boolean writeEvent(E event);
    
    /**
     * Returns the approximate queue size. Note that highly concurrent queue implementations do not guarantee exactness
     * and hence users of implementations must not rely on this. This is meant pruely for metrics. 
     * 
     * @return int
     */
    public int getQueueSize();
}
