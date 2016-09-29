package com.netflix.turbine.data.meta;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.turbine.data.DataFromSingleInstance;
import com.netflix.turbine.data.TurbineData;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;

/**
 * Simple class that tracks the metadata about a cluster monitor. 
 * For now we simply track the no of hosts that the cluster monitor is actively receiving data from in the last 10 seconds. 
 * The meta data is periodically streamed to downstream listeners using a dedicated thread. 
 * This helps keep the meta info reliability independent of the actual data streaming from upstream {@link InstanceMonitor}(s)
 * 
 * An e.g of meta info is 
 *     data:  {type:meta, name:meta, reportingHostsLast10Seconds:30} 
 *     
 * See {@link MetaInfoUpdator} for details on how and when the meta info is updated and sent downstream.
 * See {@link MetaInfoAdaptor} for details on how data from the MetaInfo is converted to the right data format in order to be 
 * streamed to interested listeners.
 * 
 * @author poberai
 * @param <K>
 */
public class MetaInformation<K extends TurbineData> {
    
    // the cluster monitor represented by this meta info
    protected final ClusterMonitor<K> clusterMonitor;
    // the adaptor for translating this info into a streamable data format
    protected final MetaInfoAdaptor<K> metaInfoAdaptor; 
    
    // track when we sent this last. This helps control the periodicity of the update
    protected final AtomicLong lastUpdateTime = new AtomicLong(0L);
    // track how many hosts we received data from in the last 10 seconds
    protected final AtomicLong numReportingHosts = new AtomicLong(0L);
    
    // constants
    public static final String Meta = "meta";
    public static final String ReportingHostsLast10Seconds = "reportingHostsLast10Seconds";
    public static final String Timestamp = "timestamp";
    
    /**
     * Public constructor
     * @param monitor
     * @param adaptor
     */
    public MetaInformation(ClusterMonitor<K> monitor, MetaInfoAdaptor<K> adaptor) {
        clusterMonitor = monitor;
        metaInfoAdaptor = adaptor;
    }
    
    /**
     * Used to determine whether we should send the data downstream.
     * @return
     */
    public boolean shouldStream() {
        return (lastUpdateTime.get() == 0L) || (System.currentTimeMillis() - lastUpdateTime.get() > 3000);
    }
    
    /**
     * Update the meta info using the custom biz logic. One can extend and/or override this method to add their own meta information
     * Note that you will also need an adaptor to translate the meta data state to a streamable data format.
     */
    public void update() {
        
        Collection<TurbineDataMonitor<DataFromSingleInstance>> allHostMonitors = clusterMonitor.getInstanceMonitors().getAllMonitors();
        
        long now = System.currentTimeMillis();
        int sum = 0; 
        
        for (TurbineDataMonitor<DataFromSingleInstance> hostMonitor : allHostMonitors) {
            // get the data from each monitor
            long lastHostUpdate = hostMonitor.getLastEventUpdateTime();
            // For hosts that reported anything in the last 10 seconds
            if (lastHostUpdate > 0L && ((now - lastHostUpdate) < 10000L)) {
                sum++;
            }
        }
        
        numReportingHosts.set(sum);
    }
    
    /**
     * Send data to downstream listeners.
     */
    public void streamDataToListeners() {
        clusterMonitor.getDispatcher().pushData(clusterMonitor.getStatsInstance(), metaInfoAdaptor.getData(this));
        lastUpdateTime.set(System.currentTimeMillis());
    }
    
    /**
     * Return hosts that received data in the last 10 seconds
     * @return long
     */
    public long getReportingHosts() {
        return numReportingHosts.get();
    }
    
    /**
     * Timestamp when the meta info was last updated
     * @return long
     */
    public long getLastUpdateTime() {
        return lastUpdateTime.get();
    }
}
