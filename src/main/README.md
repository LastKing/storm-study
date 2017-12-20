# storm学习文档

## 一、最基本storm按理
**demo1**为最基本demo，可以本地随意运行不需要安装storm
包含知识点：
1. local model 这个就是一个本地 模拟的小型storm 加载在jvm中，不需要提交到storm中
2. remote model 需要将项目打包到成jar文件，并通过storm集群运行
```shell
root@xxx> storm jar xxxx.jar com.xxx.xxx.TestTopology args1 args2 ....
```
这两个区别很重要，千万别弄混了。。。本地调试的时候，都是本地虚拟的,远程模式需要将程序打包成jar，提交远端运行

## 二、基本结构



## 三、stream group 模式
* Shuffle Grouping 或 None Grouping  随机分发
* Fields Grouping   根据Field分发
* All Grouping      有点类似订阅，上游事件可以通知下游bolt中所有task
* Global Grouping   上游spout/bolt发射的所有tuple全部进入下游bolt的同一个task，通常分配给下游最小的id
* LocalOrShuffle Grouping  当前worker内或者随机分发
* Direct Grouping   完全指定下游操作
注意区分api中的 **component Id** 与 **Field name**。我记性不好。。

基础知识
1. [官方文档](http://storm.apachecn.org/releases/cn/1.1.0/Concepts.html)
2. [民间详解](http://www.cnblogs.com/kqdongnanf/p/4634607.html)
3. 更多关于分组分发学习[进阶文章](http://zqhxuyuan.github.io/2016/06/30/Hello-Storm/)