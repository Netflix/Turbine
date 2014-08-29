package com.netflix.turbine.discovery;

import com.google.common.collect.Lists;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringListProperty;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.x.discovery.ServiceCache;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ZookeeperInstanceDiscovery
 *
 * Uses curator-x-discovery to discover nodes to poll with Turbine.
 *
 * Can monitor multiple clusters, but all must be under the same service discovery path.
 *
 * By default, will monitor /hystrix-event/{cluster}
 *
 * Archaius properties utilized:
 *
 * turbine.aggregator.clusterConfig (will monitor each cluster as a service under the serviceDiscoveryPath)
 * turbine.ZookeeperInstanceDiscovery.zookeeper.quorum (default: 127.0.0.1)
 * turbine.ZookeeperInstanceDiscovery.zookeeper.namespace (ZooKeeper namespace, none by default)
 * turbine.ZookeeperInstanceDiscovery.zookeeper.serviceDiscoveryPath (default: /hystrix-event)
 * turbine.ZookeeperInstanceDiscovery.zookeeper.connectTimeoutMs (default: 15000ms)
 *
 * Instance properties added:
 * server-port is initialized to the value of {@link com.netflix.curator.x.discovery.ServiceInstance#getPort()}
 * ^ Can be used in your instanceUrlSuffix as so:
 *
 * turbine.instanceUrlSuffix=:{server-port}/hystrix.stream
 *
 * Will only hit ZooKeeper when the watch registered on the various services triggers.
 *
 * @author Michael Rose <elementation@gmail.com>
 */
public class ZookeeperInstanceDiscovery implements InstanceDiscovery {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final CuratorFramework zk;
    private final ServiceDiscovery<Void> dsc;
    private final List<ServiceCache<Void>> serviceCaches = Lists.newArrayList();

    public ZookeeperInstanceDiscovery() throws Exception {
        List<String> clusters =
                new DynamicStringListProperty("turbine.aggregator.clusterConfig", new ArrayList<String>()).get();

        String zkQuorum = DynamicPropertyFactory.getInstance()
                .getStringProperty("turbine.ZookeeperInstanceDiscovery.zookeeper.quorum", "127.0.0.1").get();

        String zkNamespace = DynamicPropertyFactory.getInstance()
                .getStringProperty("turbine.ZookeeperInstanceDiscovery.zookeeper.namespace", null).get();

        String serviceDiscoveryPath = DynamicPropertyFactory.getInstance()
                .getStringProperty("turbine.ZookeeperInstanceDiscovery.zookeeper.serviceDiscoveryPath", "/hystrix-event").get();

        int connectTimeoutMs = DynamicPropertyFactory.getInstance()
                .getIntProperty("turbine.ZookeeperInstanceDiscovery.zookeeper.connectTimeoutMs", 15000).get();

        log.info("Initializing ZookeeperInstanceDiscovery with quorum=[{}] namespace=[{}]",
                new Object[]{zkQuorum, zkNamespace});

        zk = CuratorFrameworkFactory.builder()
                .connectString(zkQuorum)
                .connectionTimeoutMs(connectTimeoutMs)
                .retryPolicy(new ExponentialBackoffRetry(1000, 6))
                .namespace(zkNamespace)
                .build();

        zk.start();

        log.info("Initializing Service Discovery with serviceDiscoveryPath=[{}] and clusters={}",
                new Object[]{serviceDiscoveryPath, clusters});

        dsc = ServiceDiscoveryBuilder.builder(Void.class)
                .basePath(serviceDiscoveryPath)
                .client(zk)
                .build();

        dsc.start();

        for (String cluster : clusters) {
            ServiceCache<Void> serviceCache = dsc.serviceCacheBuilder()
                    .name(cluster)
                    .build();
            serviceCache.start();

            serviceCaches.add(serviceCache);
        }

    }

    @Override
    public Collection<Instance> getInstanceList() throws Exception {
        List<Instance> collectedInstances = Lists.newArrayList();

        for (ServiceCache<Void> serviceCache : serviceCaches) {
            for (ServiceInstance<Void> serviceInstance : serviceCache.getInstances()) {
                Instance instance = new Instance(
                        serviceInstance.getAddress(),
                        serviceInstance.getName(),
                        true);
                instance.getAttributes().put("server-port", serviceInstance.getPort().toString());

                collectedInstances.add(instance);
            }
        }

        return collectedInstances;
    }
}
