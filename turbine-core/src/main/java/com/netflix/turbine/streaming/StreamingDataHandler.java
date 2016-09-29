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
package com.netflix.turbine.streaming;

import java.util.Set;

/**
 * Class the represents a streaming data listener for data being sent over a {@link TurbineStreamingConnection}
 * <p>The listener must be able to 
 * <ul>
 * <li>Respond to regular data events where the data may be sorted by the streaming connection. 
 * <li>Respond to delete events for data that was higher priority earlier on but has now been superceded by another data type
 * <li>Respond to pings where there is no activity. 
 * </ul>
 * 
 * <p>Note that if the StreamingDataHandler throws an Exception during this process, then the {@link TurbineStreamingConnection} will stop. 
 * 
 */
public interface StreamingDataHandler {
    
    /**
     * Write data to your underlying stream etc
     * @param data
     * @throws Exception
     */
    public void writeData(String data) throws Exception;

    /**
     * Delete data specified for the stream type 
     * @param type
     * @param names
     * @throws Exception
     */
    public void deleteData(String type, Set<String> names) throws Exception;
    
    /**
     * Handle pings / heartbeats etc
     * @throws Exception
     */
    public void noData() throws Exception;
}
