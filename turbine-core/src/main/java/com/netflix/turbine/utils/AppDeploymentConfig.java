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
package com.netflix.turbine.utils;

import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

public class AppDeploymentConfig {

    private static DynamicStringProperty aggModeProperty = DynamicPropertyFactory.getInstance().getStringProperty("turbine.aggMode", AggregatorMode.DEFAULT.name());
    public static AggregatorMode aggMode = AggregatorMode.valueOf(aggModeProperty.get());

    public static enum AggregatorMode { 
        LOCAL_ZONE, MULTI_ZONE, DEFAULT; 
    }
    
    public static AppDeploymentConfig getInstance() {
        return instance;
    }
    
    private static AppDeploymentConfig instance = new AppDeploymentConfig(); 
    
    private AppDeploymentConfig () {
    }
    
}
