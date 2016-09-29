package com.netflix.turbine.data.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.turbine.data.AggDataFromCluster;
import com.netflix.turbine.monitor.TurbineDataMonitor;
import com.netflix.turbine.monitor.cluster.ClusterMonitor;

/**
 * Class to represent biz logic for translating {@link MetaInformation} <{@link AggDataFromCluster}> to {@link AggDataFromCluster}
 * Note that this class uses an extended and overriden version of {@link AggDataFromCluster} to make it simple and efficient to use
 * since currently 'meta' has no non-numeric attributes. 
 *  
 * @author poberai
 */
public class AggDataMetaInfoAdaptor implements MetaInfoAdaptor<AggDataFromCluster> {

    // the underling AggDataFromCluster obj 
    private final AggDataMetaInfo metaData;

    /**
     * Public constructor
     */
    public AggDataMetaInfoAdaptor(ClusterMonitor<AggDataFromCluster> cMonitor) {
        metaData = new AggDataMetaInfo(cMonitor);
    }

    @Override
    public AggDataFromCluster getData(MetaInformation<AggDataFromCluster> metaInfo) {
        metaData.update(metaInfo);
        return metaData;
    }

    /**
     * Class that represents a simpler and more efficient version of {@link AggDataFromCluster}
     * @author poberai
     */
    private class AggDataMetaInfo extends AggDataFromCluster {

        // thread safe reference to pull data from to populate the attributes
        private final AtomicReference<MetaInformation<AggDataFromCluster>> metaInfoReference; 

        /**
         * Private constructor
         * @param monitor
         */
        private AggDataMetaInfo(TurbineDataMonitor<AggDataFromCluster> monitor) {
            super(monitor, MetaInformation.Meta, MetaInformation.Meta);
            metaInfoReference = new AtomicReference<MetaInformation<AggDataFromCluster>>(null);
        }

        @Override
        public HashMap<String, Long> getNumericAttributes() {
            HashMap<String, Long> nAttrs = new HashMap<String, Long>();

            MetaInformation<AggDataFromCluster> mInfo = metaInfoReference.get();
            if (mInfo != null) {
                nAttrs.put(MetaInformation.ReportingHostsLast10Seconds, mInfo.getReportingHosts());
                nAttrs.put(MetaInformation.Timestamp, mInfo.getLastUpdateTime());
            }
            return nAttrs;
        }

        @Override
        public HashMap<String, String> getStringAttributes() {
            return null;
        }

        @Override
        public HashMap<String, Map<String, ? extends Number>> getNestedMapAttributes() {
            return null;
        }

        private void update(MetaInformation<AggDataFromCluster> mInfo) {
            this.metaInfoReference.set(mInfo);
        }
    }
}
