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
package com.netflix.turbine.monitor;

import java.util.concurrent.ConcurrentHashMap;

import com.netflix.turbine.data.StatsRollingNumber;
import com.netflix.turbine.data.StatsRollingNumber.Type;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.handler.TurbineDataDispatcher;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;
import com.netflix.turbine.monitor.instance.InstanceMonitor;

/**
 * Base class of a monitor that can be extended by other implementations such as {@link InstanceMonitor} and {@link ClusterMonitor}
 *
 * @param <K>
 */
public abstract class TurbineDataMonitor<K extends TurbineData> {
    
    /* used to track rejections, processing, etc */
    private StatsRollingNumber counter = new StatsRollingNumber(10000, 10);
    
    // StatsType.getName : APIMonitorRollingNumber
    private ConcurrentHashMap<String, StatsRollingNumber> numbersWith2minuteWindow = new ConcurrentHashMap<String, StatsRollingNumber>();
    
    /**
     * @return String
     */
    public abstract String getName();
    
    /**
     * Start monitoring for data
     * @throws Exception
     */
    public abstract void startMonitor() throws Exception;
    
    /**
     * Stop monitoring for data and signal shutdown to any listeners interested in data
     */
    public abstract void stopMonitor();

    /**
     * @return {@link TurbineDataDispatcher}<K>
     */
    public abstract TurbineDataDispatcher<K> getDispatcher();
    
    /**
     * @return {@link Instance}
     */
    public abstract Instance getStatsInstance();

    public void markEventDiscarded() {
        counter.increment(Type.EVENT_DISCARDED);
    }

    public void markEventProcessed() {
        counter.increment(Type.EVENT_PROCESSED);
    }

    public int getEventDiscarded() {
        return counter.getCount(Type.EVENT_DISCARDED);
    }
    
    public int getEventProcessed() {
        return counter.getCount(Type.EVENT_PROCESSED);
    }

    public StatsRollingNumber getRolling2MinuteStats(TurbineData data) {
        StatsRollingNumber rollingNumber = numbersWith2minuteWindow.get(data.getName());
        if (rollingNumber == null) {
            int statsWindow = 60000 * 2; // 2 minutes
            int numberOfBuckets = statsWindow / 1000; // 1 second windows
            rollingNumber = new StatsRollingNumber(statsWindow, numberOfBuckets);
            StatsRollingNumber previous = numbersWith2minuteWindow.putIfAbsent(data.getName(), rollingNumber);
            if (previous != null) {
                // another thread beat us to making this so we'll use this one instead
                rollingNumber = previous;
            }
        }
        return rollingNumber;
    }
    
    /**
     * Method that determines when this monitor last received an update. 
     * This is used for checking and reaping stale monitors, especially due to 
     * stale connections in the cloud. 
     * 
     * 
     * @return -1 as a default to opt out from implementing this functionality. Else return the timestamp in millis
     * marking the last update occurance. 
     */
    public long getLastEventUpdateTime() {
        return -1L;
    }
}
