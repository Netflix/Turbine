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
package com.netflix.turbine.discovery.consul;

public class ConsulInstance {

    public static enum Status {
        UP, DOWN
    }

    private final String cluster;
    private final Status status;
    private final String hostname;
    private final int port;

    public ConsulInstance(String cluster, Status status, String hostname, int port) {
        this.cluster = cluster;
        this.status = status;
        this.hostname = hostname;
        this.port = port;
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

    public String getId() {
        return hostname + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsulInstance)) return false;

        ConsulInstance that = (ConsulInstance) o;

        if (port != that.port) return false;
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
        return result;
    }

    @Override
    public String toString() {
        return "ConsulInstance{" +
                "cluster='" + cluster + '\'' +
                ", status=" + status +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                '}';
    }
}
