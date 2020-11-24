## 0 准备工作



**默认情况下**，kafka自动创建的主题的**分区数量为1**，所以我们需要先修改分区数量，来让自定义分区器有点用。

1. 先cd到`opt\kafka\bin\`

2. 运行命令：

   ````shell
   ./kafka-topics.sh --zookeeper 47.94.139.116:2181/kafka --alter --topic sun --partitions 4
   ````

3. 查看是否修改成功

   ```shell
   ./kafka-topics.sh --describe --zookeeper 47.94.139.116:2181/kafka --topic sun
   ```



![](https://healthlung.oss-cn-beijing.aliyuncs.com/20201124201605.png)



或者也可以通过修改`server.properties`中的`nums.partions`并重启，来更改默认分区数量。







