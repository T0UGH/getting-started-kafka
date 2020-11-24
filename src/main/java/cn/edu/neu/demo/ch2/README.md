# README




> 参考
>
> Kafka 2.5.0基于Docker成功部署案例:<https://www.orchome.com/8183>
>
> docker安装kafka:<https://www.jianshu.com/p/e8c29cba9fae>



我这里给出一个可行的基于docker的安装方案。它可以在一个装有docker的阿里云机器中成功安装zookeeper和kafka，并且可以通过公网地址访问到。按照步骤执行命令即可。



**注意**：如果你的安装环境也是阿里云这类的云虚机，记得在防火墙中打开如下端口:

1. 2181
2. 2888
3. 3888
4. 9092



## 0 目录结构

````
   
└─README.md
````



## 1 准备工作



### 第一步 安装zookeeper3.5.7容器



```shell
docker run -d --cpus 0.5 -m 200M \
--name zookeeper \
--restart always \
-u root \
-p 2181:2181 \
-p 2888:2888 \
-p 3888:3888 \
-v /etc/localtime:/etc/localtime \
-v /mnt/zookeeper-3.5.7/data:/data \
-v /mnt/zookeeper-3.5.7/log:/datalog \
-v /mnt/zookeeper-3.5.7/conf:/conf \
zookeeper:3.5.7
```



### 第二步 安装kafka2.5.0容器



````shell
docker run -d \
--restart always \
--name kafka-2.5.0 \
-p 9092:9092 \
--link zookeeper \
-e KAFKA_ZOOKEEPER_CONNECT=47.94.139.116:2181/kafka \
-e KAFKA_ADVERTISED_HOST_NAME=47.94.139.116 \
-e KAFKA_ADVERTISED_PORT=9092 \
-e KAFKA_DELETE_TOPIC_ENABLE=true \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
-e KAFKA_LOG_DIRS="/kafka/kafka-logs-2.5.0" \
-v /etc/localtime:/etc/localtime \
-v /mnt/kafka-2.5.0:/kafka \
-v /var/run/docker.sock:/var/run/docker.sock \
wurstmeister/kafka:2.12-2.5.0
````



**注意！！！注意！！！**上面的命令中

1. 请将`KAFKA_ADVERTISED_HOST_NAME=47.94.139.116`修改为你本机的ip地址或者hostname
2. 请将`KAFKA_ZOOKEEPER_CONNECT=47.94.139.116:2181/kafka`修改为你第一步安装的zookeeper所在机器ip地址



### 第三步 查看server.properties



```shell
docker exec -it kafka-2.5.0 /bin/sh
```



```shell
cat opt\kafka\config\server.properties
```



````properties
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# see kafka.server.KafkaConfig for additional details and defaults

############################# Server Basics #############################

# The id of the broker. This must be set to a unique integer for each broker.
broker.id=-1

############################# Socket Server Settings #############################

# The address the socket server listens on. It will get the value returned from 
# java.net.InetAddress.getCanonicalHostName() if not configured.
#   FORMAT:
#     listeners = listener_name://host_name:port
#   EXAMPLE:
#     listeners = PLAINTEXT://your.host.name:9092
#listeners=PLAINTEXT://:9092

# Hostname and port the broker will advertise to producers and consumers. If not set, 
# it uses the value for "listeners" if configured.  Otherwise, it will use the value
# returned from java.net.InetAddress.getCanonicalHostName().
#advertised.listeners=PLAINTEXT://your.host.name:9092

# Maps listener names to security protocols, the default is for them to be the same. See the config documentation for more details
#listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL

# The number of threads that the server uses for receiving requests from the network and sending responses to the network
num.network.threads=3

# The number of threads that the server uses for processing requests, which may include disk I/O
num.io.threads=8

# The send buffer (SO_SNDBUF) used by the socket server
socket.send.buffer.bytes=102400

# The receive buffer (SO_RCVBUF) used by the socket server
socket.receive.buffer.bytes=102400

# The maximum size of a request that the socket server will accept (protection against OOM)
socket.request.max.bytes=104857600


############################# Log Basics #############################

# A comma separated list of directories under which to store log files
log.dirs=/kafka/kafka-logs-2.5.0

# The default number of log partitions per topic. More partitions allow greater
# parallelism for consumption, but this will also result in more files across
# the brokers.
num.partitions=1

# The number of threads per data directory to be used for log recovery at startup and flushing at shutdown.
# This value is recommended to be increased for installations with data dirs located in RAID array.
num.recovery.threads.per.data.dir=1

############################# Internal Topic Settings  #############################
# The replication factor for the group metadata internal topics "__consumer_offsets" and "__transaction_state"
# For anything other than development testing, a value greater than 1 is recommended to ensure availability such as 3.
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

############################# Log Flush Policy #############################

# Messages are immediately written to the filesystem but by default we only fsync() to sync
# the OS cache lazily. The following configurations control the flush of data to disk.
# There are a few important trade-offs here:
#    1. Durability: Unflushed data may be lost if you are not using replication.
#    2. Latency: Very large flush intervals may lead to latency spikes when the flush does occur as there will be a lot of data to flush.
#    3. Throughput: The flush is generally the most expensive operation, and a small flush interval may lead to excessive seeks.
# The settings below allow one to configure the flush policy to flush data after a period of time or
# every N messages (or both). This can be done globally and overridden on a per-topic basis.

# The number of messages to accept before forcing a flush of data to disk
#log.flush.interval.messages=10000

# The maximum amount of time a message can sit in a log before we force a flush
#log.flush.interval.ms=1000

############################# Log Retention Policy #############################

# The following configurations control the disposal of log segments. The policy can
# be set to delete segments after a period of time, or after a given size has accumulated.
# A segment will be deleted whenever *either* of these criteria are met. Deletion always happens
# from the end of the log.

# The minimum age of a log file to be eligible for deletion due to age
log.retention.hours=168

# A size-based retention policy for logs. Segments are pruned from the log unless the remaining
# segments drop below log.retention.bytes. Functions independently of log.retention.hours.
#log.retention.bytes=1073741824

# The maximum size of a log segment file. When this size is reached a new log segment will be created.
log.segment.bytes=1073741824

# The interval at which log segments are checked to see if they can be deleted according
# to the retention policies
log.retention.check.interval.ms=300000

############################# Zookeeper #############################

# Zookeeper connection string (see zookeeper docs for details).
# This is a comma separated host:port pairs, each corresponding to a zk
# server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002".
# You can also append an optional chroot string to the urls to specify the
# root directory for all kafka znodes.
zookeeper.connect=47.94.139.116:2181/kafka

# Timeout in ms for connecting to zookeeper
zookeeper.connection.timeout.ms=18000


############################# Group Coordinator Settings #############################

# The following configuration specifies the time, in milliseconds, that the GroupCoordinator will delay the initial consumer rebalance.
# The rebalance will be further delayed by the value of group.initial.rebalance.delay.ms as new members join the group, up to a maximum of max.poll.interval.ms.
# The default value for this is 3 seconds.
# We override this to 0 here as it makes for a better out-of-the-box experience for development and testing.
# However, in production environments the default value of 3 seconds is more suitable as this will help to avoid unnecessary, and potentially expensive, rebalances during application startup.
group.initial.rebalance.delay.ms=0

advertised.port=9092
advertised.host.name=47.94.139.116
delete.topic.enable=true
port=9092
````

- 并没有`listeners=PLAINTEXT://:9092`





