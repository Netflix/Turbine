/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.discovery.eureka;

import com.netflix.eureka2.interests.ChangeNotification;
import com.netflix.eureka2.registry.InstanceInfo;
import com.netflix.eureka2.registry.ServicePort;
import com.netflix.eureka2.registry.NetworkAddress.ProtocolType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class EurekaInstance {

    public static enum Status {
        UP, DOWN
    }

    private final String cluster;
    private final Status status;
    private final String hostname;
    private final int port;
    private final Map<String, Object> attributes;

    private EurekaInstance(String cluster, Status status, String hostname, int port) {
        this.cluster = cluster;
        this.status = status;
        this.hostname = hostname;
        this.port = port;
        this.attributes = new HashMap<>();
    }

    public static EurekaInstance from(ChangeNotification<InstanceInfo> notification) {
        InstanceInfo instanceInfo = notification.getData();
        String cluster = instanceInfo.getApp();
        
        String ipAddress = instanceInfo.getDataCenterInfo()
                .getAddresses().stream()
                .filter(na -> na.getProtocolType() == ProtocolType.IPv4)
                .collect(Collectors.toList()).get(0).getIpAddress();
        HashSet<ServicePort> servicePorts = instanceInfo.getPorts();
        int port = instanceInfo.getPorts().iterator().next().getPort();

        Status status = ChangeNotification.Kind.Delete == notification.getKind()
                ? Status.DOWN  // count deleted as DOWN
                : (InstanceInfo.Status.UP == instanceInfo.getStatus() ? Status.UP : Status.DOWN);

        return new EurekaInstance(cluster, status, ipAddress, port);
    }

    public Status getStatus() {
        return status;
    }

    public String getCluster() {
        return cluster;
    }

    public String getHost() {
        return hostname;
    }

    public boolean isUp() {
        return Status.UP == status;
    }
    
    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EurekaInstance)) return false;

        EurekaInstance that = (EurekaInstance) o;

        if (port != that.port) return false;
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
        if (cluster != null ? !cluster.equals(that.cluster) : that.cluster != null) return false;
        if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = cluster != null ? cluster.hashCode() : 0;
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EurekaInstance{" +
                "cluster='" + cluster + '\'' +
                ", status=" + status +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                ", attributes=" + attributes +
                '}';
    }
}
