Redis-Keeper
----
Redis-Keeper is a distributed system that monitor and auto failover multiple Redis clusters.

**It is designed to work with any client/language.**

----

##### What about Redis Sentinel?
Similarly to redis-sentinel it is also a external process that checks the healthy of each redis, and can auto assign a new master in case of failures. It is also design to run in multiple nodes to present a safer[1] view of the system.

The difference is on the way that those external processes get in a consensus, and how clients get in sync with the current state of the system.

----

[1] Network partition is something that redis-sentinel is proven to not quite handle, and that i'll try to improve on the next versions of redis-keeper. Even so, keep in mind that a strict CP system is not the intention of this project.

To get more info on redis-sentinel under network partition: [Jepsen redis-sentinel test](http://aphyr.com/posts/283-call-me-maybe-redis) and [Antirez reply](http://antirez.com/news/55)

----

### How it Works:

When the redis-keeper starts, it will elect one of the online instances as a coordinator Leader. The leader job is to detect when the majority of the system marked a Redis as down, and to trigger a failover process to handle it.
It is built using [ZooKeeper](http://zookeeper.apache.org/).

On Leader startup, it will try to automatically identify the Redis cluster configuration(roles, status), and do some role reconfiguration if needed(like multiple redis master).

##### What happen when a redis instance goes down?
Eventually the majority of online redis-keepers will detect the redis instance as down(this may take few seconds, depending on configuration, more bellow). 

Only then the Leader will act, updating the node status, and trigger a failover process if it had a master role. 

This failover process looks for the better[2] surviving slave, and promote it to a master(also update others slave to point to the new master). In case of no good slave available, the failover will fails, and the redis cluster will stay down until a good node is available to become master.

The leader also take care of update the cluster status on ZooKeeper, so that clients receive the changes.

----

[2] Choosing the best slave can be tricky, currently we choose the one with highest uptime, which is not optimal, on future versions we will use the replication offset available on redis 2.8.

----

### Clients 

The project supports a generic approach to the client. Using a rest api, it supports [HAProxy](http://haproxy.1wt.eu/) as a proxy to the proper redis instance.

That way it is language and client independent, and the performance loss is acceptable for most cases(20% loss in our tests).
A ZooKeeper based client is on the queue for future versions, to achieve maximum performance.

Now we recommend running a local haproxy service, and connect your process to the local haproxy port.
See [example configuration](#haproxy-example-configuration) for more detail how to configure haproxy.

----

### Configuration
```json
{
  "keeper-id": "keeper-1",
  "zk-quorum": ["zknode1.dev:2181", "zknode2.dev:2181"],
  "zk-prefix": "/rediskeeper",
  "rest-port": 46379,
  "tick-seconds": 1,
  "failover-tick-seconds": 10,
  "seconds-mark-down": 5,
  "clusters": [
    {
      "name": "cluster-1", 
      "nodes": [
        {"host": "192.168.1.40", "port": 6379},
        {"host": "192.168.1.50", "port": 6379},
        {"host": "192.168.1.60", "port": 6379},
      ]
    },
    {
      "name": "cluster-2",
      "nodes": [
        {"host": "192.168.2.40", "port": 6380},
        {"host": "192.168.2.50", "port": 6380},
        {"host": "192.168.2.60", "port": 6380},
        {"host": "192.168.2.70", "port": 6380},
      ]
    }
  ]
}
```

- `keeper-id`: Unique id used for this redis-keeper(required to be unique across instances)
- `zk-quorum`: list of zookeeper servers
- `zk-prefix`: zookeeper prefix
- `rest-port`: port to be used on rest api
- `tick-seconds`: seconds between redis health check
- `failover-tick-seconds`: seconds between failover check/execution
- `seconds-mark-down`: seconds that a redis node can stay inaccessible before being mark as down
- `clusters`: list of cluster to be monitored, each cluster has a unique name, and a unique list of redis server.

#### HAProxy example configuration:

```conf
# Redis-Keeper (cause redis-keeper must be high-available too)
frontend redis-keeper
  # this will be used internally to check nodes roles
  bind *:4380 
  default_backend redis-keeper

backend redis-keeper
  mode http
  balance roundrobin
  # Configuring all redis-keeper:
  server 192.169.1.1:4379 192.169.1.1:4379 check
  server 192.169.1.2:4379 192.169.1.2:4379 check
  server 192.169.1.3:4379 192.169.1.3:4379 check
```

```conf
# Cluster 1 writable(redis master)
frontend cluster-1-writable
  mode tcp # required for redis
  # your client should use this port to connect to the master
  bind *:1301
  default_backend cluster-1-writable

backend cluster-1-writable
  mode tcp # required for redis
  balance static-rr

  # need this to haproxy send node info to redis-keeper
  http-check send-state 

  # override haproxy check, it is important to use correct cluster name on url
  option httpchk GET /cluster/cluster-1/is-writable HTTP/1.1\r\nHost:\ localhost

  # all redis server in this cluster, must use same name as configured on redis-keeper.
  # also change check address to use redis-keeper haproxy port
  server 192.168.1.40:6379 192.168.1.40:6379 check inter 10s addr 127.0.0.1 port 4380
  server 192.168.1.50:6379 192.168.1.50:6379 check inter 10s addr 127.0.0.1 port 4380
  server 192.168.1.60:6379 192.168.1.60:6379 check inter 10s addr 127.0.0.1 port 4380
```

```conf
# Redis readable cluster-1(in case you want to read from slaves too):
frontend cluster-1-readable
  mode tcp
  # your client can access this port to read from any online node(slaves + master):
  bind *:9301 
  default_backend cluster-1-readable

backend cluster-1-readable
  mode tcp
  balance static-rr
  http-check send-state

  # Pretty much the same configuration from writable backend, only the url changes
  option httpchk GET /cluster/cluster-1/is-readable HTTP/1.1\r\nHost:\ localhost

  server 192.168.1.40:6379 192.168.1.40:6379 check inter 10s addr 127.0.0.1 port 4380
  server 192.168.1.50:6379 192.168.1.50:6379 check inter 10s addr 127.0.0.1 port 4380
  server 192.168.1.60:6379 192.168.1.60:6379 check inter 10s addr 127.0.0.1 port 4380
```

![Haproxy configuration result](docs/imgs/haproxy.png "Haproxy Config")

----

### Using:
You can get the last version at: [Download 0.1](http://github.com/rodrigopr/redis-keeper/releases/download/untagged-1fd23d8d661be18c3db0/redis-keeper-0.1-bin.zip).
To start the system execute: `bin/redis-keeper.sh conf/keeper.conf`

##### To build from source(maven required): 
```bash 
git clone http://github.com/rodrigopr/redis-keeper.git
cd redis-keeper
mvn clean compile package
```

It will produce .zip package in `target/`.

The tests requires redis-server to be installed on your system, add `-DskipTests=true` to end of `mvn` command to disable test execution.

-----

### Todo:
- **Zookeeper based clients**
- **Support add redis instance to a cluster** - Today requires restart every redis-keeper.
- **Support standby redis instances** - Independent nodes that can be used in any cluster when it is lacking slaves.
- **Improve slave->master election** - use replication offset of redis 2.8(make it configurable)
- **Improve Rest API** - support manual operations (change leader,  add node, rem node, force failover)
- **Improve failover process** - today the leader must be able to access the slaves to process the failover, no matter if the majority think its up. This could lead to some nasty network partition problems, perhaps we should rethink the way that failover happens, to not rely too much on the leader.
- **Stronger configuration check**, sync all redis-keeper to have the same configuration(clusters info, timers, etc)

----

### License

This software is licensed under the Apache 2 license, quoted below.

Licensed under the Apache License, Version 2.0 (the ?License?); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an ?AS IS? BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.