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

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;

public class EurekaInstance {

    public static enum Status {
        UP, DOWN
    }

    private final Status status;
    private final InstanceInfo instance;

    private EurekaInstance(Status status, InstanceInfo instance) {
        this.status = status;
        this.instance = instance;
    }

    public static EurekaInstance create(InstanceInfo instance) {
        Status status;
        if (InstanceStatus.UP == instance.getStatus()) {
            status = Status.UP;
        } else {
            status = Status.DOWN;
        }
        return new EurekaInstance(status, instance);
    }

    public static EurekaInstance create(Status status, InstanceInfo instance) {
        return new EurekaInstance(status, instance);
    }

    public Status getStatus() {
        return status;
    }

    public InstanceInfo getInstanceInfo() {
        return instance;
    }

    public String getAppName() {
        return instance.getAppName();
    }

    public String getHostName() {
        return instance.getHostName();
    }

    public String getIPAddr() {
        return instance.getIPAddr();
    }

    public String getVIPAddress() {
        return instance.getVIPAddress();
    }

    public String getASGName() {
        return instance.getASGName();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getAppName() == null) ? 0 : getAppName().hashCode());
        result = prime * result + ((getASGName() == null) ? 0 : getASGName().hashCode());
        result = prime * result + ((getHostName() == null) ? 0 : getHostName().hashCode());
        result = prime * result + ((getIPAddr() == null) ? 0 : getIPAddr().hashCode());
        result = prime * result + ((getVIPAddress() == null) ? 0 : getVIPAddress().hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EurekaInstance other = (EurekaInstance) obj;
        if (getAppName() == null) {
            if (other.getAppName() != null)
                return false;
        } else if (!getAppName().equals(other.getAppName()))
            return false;
        if (getASGName() == null) {
            if (other.getASGName() != null)
                return false;
        } else if (!getASGName().equals(other.getASGName()))
            return false;
        if (getHostName() == null) {
            if (other.getHostName() != null)
                return false;
        } else if (!getHostName().equals(other.getHostName()))
            return false;
        if (getIPAddr() == null) {
            if (other.getIPAddr() != null)
                return false;
        } else if (!getIPAddr().equals(other.getIPAddr()))
            return false;
        if (getVIPAddress() == null) {
            if (other.getVIPAddress() != null)
                return false;
        } else if (!getVIPAddress().equals(other.getVIPAddress()))
            return false;
        if (status != other.status)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "EurekaInstance [status=" + status + ", vip=" + instance.getVIPAddress() + ", hostname=" + instance.getHostName() + ", asg=" + instance.getASGName() + "]";
    }

}
