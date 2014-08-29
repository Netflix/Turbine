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

import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.netflix.config.DynamicPropertyFactory;

/**
 * A utility class for querying information about AWS autoscaling groups and EC2 instances using the AWS APIs.
 * 
 * Converts EC2 Instance types into Turbine Instance types for use by the Turbine engine.
 * 
 * @author Chris Fregly (chris@fregly.com)
 */
public class AwsUtil {
    private final Logger logger = LoggerFactory.getLogger(AwsUtil.class);

    private final AmazonAutoScalingClient asgClient;
    private final AmazonEC2Client ec2Client;
    
    public AwsUtil() {
    	asgClient = new AmazonAutoScalingClient();
    	ec2Client = new AmazonEC2Client();
    	
    	String endpoint = "autoscaling." + DynamicPropertyFactory.getInstance().getStringProperty("turbine.region", "us-east-1").get() + ".amazonaws.com";    	
    	asgClient.setEndpoint(endpoint);    	
    	logger.debug("Set the asgClient endpoint to [{}]", endpoint);
    }

    /**
     * Queries AWS to get the autoscaling information given the asgName.
     * 
     * @param asgName 
     * @return - AWS ASG info
     */
    public AutoScalingGroup getAutoScalingGroup(String asgName) {
        // allows multiple asg names
        DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(asgName);
        DescribeAutoScalingGroupsResult result = asgClient.describeAutoScalingGroups(request);
        List<AutoScalingGroup> asgs = result.getAutoScalingGroups();

        AutoScalingGroup asg = null;
        if (!asgs.isEmpty()) {
            // retrieve the 1st
            asg = asgs.get(0);
        } 
        
    	logger.debug("retrieved asg [{}] for asgName [{}]", asg, asgName);

        return asg;
    }

    /**
     * Convert from AWS ASG Instances to Turbine Instances
     * 
     * @param asgName
     * @return list of Turbine Instances (not AWS Instances)
     */
	public List<Instance> getTurbineInstances(String asgName) {
        List<com.amazonaws.services.autoscaling.model.Instance> awsInstances = getAutoScalingGroup(asgName).getInstances();
		
		Collection<String> instanceIds =   
            Collections2.transform(awsInstances, new Function<com.amazonaws.services.autoscaling.model.Instance, String>(){
            	@Override  
            	public String apply(com.amazonaws.services.autoscaling.model.Instance asgInstance)  
            	{  
            		return asgInstance.getInstanceId();  
            	}  
        	});  
    	
		DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();    	
    	describeInstancesRequest.withInstanceIds(instanceIds);

    	DescribeInstancesResult describeInstancesResult = ec2Client.describeInstances(describeInstancesRequest);
    	List<Reservation> reservations = describeInstancesResult.getReservations();
    	List<Instance> turbineInstances = new ArrayList<Instance>();
    	
    	// add all instances from each of the reservations - after converting to Turbine instance
    	for (Reservation reservation : reservations) {
    		List<com.amazonaws.services.ec2.model.Instance> ec2Instances = reservation.getInstances();
    		for (com.amazonaws.services.ec2.model.Instance ec2Instance : ec2Instances) {
    			String hostname = ec2Instance.getPublicDnsName();
    			
    			String statusName = ec2Instance.getState().getName();
    			boolean status = statusName.equals("running"); // see com.amazonaws.services.ec2.model.InstanceState for values
    			
    			Instance turbineInstance = new Instance(hostname, asgName, status);
    			turbineInstance.getAttributes().put("asg", asgName);
    			
    			turbineInstances.add(turbineInstance);
    		}    		
    	}

    	return turbineInstances;
	}
}
