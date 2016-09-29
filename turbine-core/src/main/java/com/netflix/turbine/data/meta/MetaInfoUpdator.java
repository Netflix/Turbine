package com.netflix.turbine.data.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;

/**
 * Class that encapsulates a scheduled updator thread responsible for periodically sending meta info updates to interested parties.
 * It uses a timer task that runs once every second and sends updates downstream.
 * The updator uses the register interface to discover the list of {@link MetaInformation} to update.
 *
 * One can turn off this feature entirely by using the archaius property 'turbine.MetaInfoUpdator.enabled'
 * and one can control the frequency of the timer task using 'turbine.MetaInfoUpdator.runMillis'
 *
 * @author poberai
 */
public class MetaInfoUpdator {

    // Fast property to turn meta updates ON/OFF
    private static final DynamicBooleanProperty UpdatorEnabled = DynamicPropertyFactory.getInstance().getBooleanProperty("turbine.MetaInfoUpdator.enabled", true);
    private static final DynamicIntProperty UpdatorFrequencyMillis = DynamicPropertyFactory.getInstance().getIntProperty("turbine.MetaInfoUpdator.runMillis", 1000);

    // timer
    private final Timer timer = new Timer();
    // thread safe reference to the list of MetaInformation
    private final AtomicReference<List<MetaInformation<?>>> metaInfoReference
        = new AtomicReference<List<MetaInformation<?>>>(new ArrayList<MetaInformation<?>>());

    // the singleton instance
    public static final MetaInfoUpdator Instance = new MetaInfoUpdator();

    /**
     * Private constructor
     */
    private MetaInfoUpdator() {
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                // Don't run if this is turned off.
                if (!UpdatorEnabled.get()) {
                    return;
                }
                for (MetaInformation<?> metaInfo : metaInfoReference.get()) {
                    if (metaInfo.shouldStream()) {
                        metaInfo.update();
                        metaInfo.streamDataToListeners();
                    }
                }
            }

        }, 1000, UpdatorFrequencyMillis.get());
    }

    /**
     * Register {@link MetaInformation} to be updated
     * @param metaInfo
     */
    public static void addMetaInfo(MetaInformation<?> metaInfo) {
        if (metaInfo == null) {
            return;
        }
        List<MetaInformation<?>> newList = new ArrayList<MetaInformation<?>>(Instance.metaInfoReference.get());
        newList.add(metaInfo);
        Instance.metaInfoReference.set(newList);
    }

    /**
     * De0register {@link MetaInformation}
     * @param metaInfo
     */
    public static void removeMetaInfo(MetaInformation<?> metaInfo) {
        if (metaInfo == null) {
            return;
        }
        List<MetaInformation<?>> newList = new ArrayList<MetaInformation<?>>(Instance.metaInfoReference.get());
        newList.remove(metaInfo);
        Instance.metaInfoReference.set(newList);
    }

    public void stop() {
        timer.cancel();
    }
}


