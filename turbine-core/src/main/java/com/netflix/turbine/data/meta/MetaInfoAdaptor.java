package com.netflix.turbine.data.meta;

import com.netflix.turbine.data.TurbineData;

/**
 * Interface to represent the logic to translate {@link MetaInformation} <K> to data K which can be then sent to listeners downstream.
 * 
 * @author poberai
 * @param <K>
 */
public interface MetaInfoAdaptor<K extends TurbineData> {
    
    /**
     * Get streamable data K from {@link MetaInformation}
     * @param metaInfo
     * @return K
     */
    public K getData(MetaInformation<K> metaInfo);
}
