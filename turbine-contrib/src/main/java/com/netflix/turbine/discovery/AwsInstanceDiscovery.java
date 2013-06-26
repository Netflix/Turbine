/*
 * Copyright 2013 Chris Fregly (chris@fregly.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.netflix.turbine.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.netflix.config.DynamicPropertyFactory;

/**
 * Class that encapsulates an {@link InstanceDiscovery} implementation that uses AWS directly to query the instances from a given ASG.
 * 
 * This plugin requires a list of ASG names specified using the {@link InstanceDiscovery#TURBINE_AGGREGATOR_CLUSTER_CONFIG} property.  
 * It then queries the set of instances for each ASG provided. 
 * 
 * Instance information retrieved from AWS must be translated to something that Turbine can understand i.e the {@link Instance} class.
 * This translation can be found in the {@link AwsUtil} class.
 * 
 * @author cfregly
 */
public class AwsInstanceDiscovery implements InstanceDiscovery {    
    private static final Logger logger = LoggerFactory.getLogger(AwsInstanceDiscovery.class);

    // ASGs to be queried
    private final String asgNames = DynamicPropertyFactory.getInstance().getStringProperty(TURBINE_AGGREGATOR_CLUSTER_CONFIG, "").get();
    private final List<String> asgNameList = new ArrayList<String>();
    
    private static final Splitter SPLITTER = Splitter.on(',')
		       .trimResults()
		       .omitEmptyStrings();
    
    private final AwsUtil awsUtil;
    
    public AwsInstanceDiscovery() {
        Iterable<String> asgNamesIter = SPLITTER.split(asgNames);
        for (String asgName : asgNamesIter) {
        	asgNameList.add(asgName);
        }

        logger.info("Fetching instance list for asgs: " + asgNameList);

        awsUtil = new AwsUtil();
    }
    
    /**
     * Method that queries AWS for a list of instances for the configured ASG names
     * 
     * @return collection of Turbine instances
     */
    @Override
    public Collection<Instance> getInstanceList() throws Exception {
        List<Instance> instances = new ArrayList<Instance>();
        
        for (String asgName : asgNameList) {
            try {
                instances.addAll(getInstancesForAsg(asgName));
            } catch (Exception e) {
                logger.error("Failed to fetch instances for asg: " + asgName + ", retrying once more", e);
                try {
                    instances.addAll(getInstancesForAsg(asgName));
                } catch (Exception e1) {
                    logger.error("Failed again to fetch instances for asg: " + asgName + ", giving up", e);
                }
            }
        }
        return instances;
    }
    
    /**
     * Private helper that fetches the Instances for each asg.
     * 
     * @param asgName
     * @return collection of Turbine instances for a particular asg using the {@link AwsUtil} helper.
     * 
     * @throws Exception
     */
    private List<Instance> getInstancesForAsg(String asgName) throws Exception {
    	logger.info("Fetching instances for asg: " + asgName);
                
    	return awsUtil.getTurbineInstances(asgName); 
    }    
}