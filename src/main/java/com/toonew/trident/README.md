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
