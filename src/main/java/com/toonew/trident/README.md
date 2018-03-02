# trident storm 知识梗概
（呆整理，这里有点问题）

## 1.基本流程
storm对数据的处理流程举例：
1. 将待处理的信息进行通过spout进行处理，转化成stream
2. 将stream切分成大量的batch（包含大量tuple），使用 Function,Map,Filter 对tuple中的数据进行处理
3. aggregate将数据进行聚合


storm 工作还有partition
storm中的任务Function，Map，Filter是散落在不同的partition进行处理的，
只有最后的persistent聚合的时候才会将所有的partition中的所有数据进行聚合


storm 工作流程模型 [参考](http://storm.apachecn.org/releases/cn/1.1.0/Understanding-the-parallelism-of-a-Storm-topology.html)
1. 将输入流转成stream
2. 将stream拆封分成多个batch，batch中包含多个tuple

区分 work executors task
1. 一个 work 运行执行 指定的topology
2. 将topology中的一个或者多个spout/blots运行一个或者多个executors


work 产生 executors 产生task


# trident storm 状态管理
注意：（深坑千万别弄混了，或许只有我这种眼瞎的白痴）
1. partitionPersist 和 persistentAggregate 两者才返回TridentState对象
2. aggregate 和 partitionAggregate  两者返回的stream

由以上两点进行了一下api实际的操作的猜想：
* 可以看出 aggregate 和partitionAggregate是对stream 的操作，应该发生在partition的
对流的操作而已，**state**并不是通过不是用这两个api进行管理的，具体怎么样还呆商榷
* partitionPersist和persistentAggregate两者都是对state进行管理的，前者更加基础，
大量的功能需要自己去实现，后者帮你实现了大量的功能，只需要按照它提供的实现，按部就班

其中state包含以下操作：
* update  (partition不会自动，persistent会自动保存，描述的好像有问题，具体看文档）
* get/query


# trident kafka 结合
[官方文档](https://github.com/apache/storm/tree/master/external/storm-kafka)<br/>
[官方文档民间翻译--中间可能有错误，需要注意](http://blog.csdn.net/jinhong_lu/article/details/46766195)<br/>

配置kafka，生成spout，设置事务等级
1. schema 设置kafka消息的处理模式
2. offset 存储和恢复kafka topic的偏移量
    * 最早      --   0
    * 最新      --   last
    * unix时间戳 --  zookeeper   （不知道我这么理解的又没有错误）
    * 以unix时间戳格式优先级最高，每次都会默认调用zookeeper中的offset，如果存在的话。
    依据，官方文档how KafkaSpout stores offset of a kafka topic and recovers in case of failures?
    这已大段，[民间翻译](https://ask.helplib.com/storm/post_4470919)
3. 因为offset信息保存在zookeeper的`SpoutConfig.zkRoot+ "/" + SpoutConfig.id`路径下，
所以在重启topology时，需要保证`SpoutConfig.zkRoot` and `SpoutConfig.id`
4. 还有ignoreZkOffsets 是否跳过zkOffset
