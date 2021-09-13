[English](./README.md) | 简体中文

# Gobeeq
[Bee-Queue](https://github.com/bee-queue/bee-queue)的Go语言实现。由Redis支持的一个简单，快速，健壮的作业/任务队列。

[![Ci](https://github.com/elvinchan/gobeeq/actions/workflows/ci.yml/badge.svg)](https://github.com/elvinchan/gobeeq/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/elvinchan/gobeeq/branch/master/graph/badge.svg)](https://codecov.io/gh/elvinchan/gobeeq)
[![Go Report Card](https://goreportcard.com/badge/github.com/elvinchan/gobeeq)](https://goreportcard.com/report/github.com/elvinchan/gobeeq)
[![Go Reference](https://pkg.go.dev/badge/github.com/elvinchan/gobeeq.svg)](https://pkg.go.dev/github.com/elvinchan/gobeeq)
[![MPLv2 License](https://img.shields.io/badge/license-MPLv2-blue.svg)](https://www.mozilla.org/MPL/2.0/)

## 停滞集
Bee-Queue尽可能提供["at least once delivery"](http://www.cloudcomputingpatterns.org/At-least-once_Delivery)。任何入队的Job应该被处理至少一次，如果Worker崩溃，离线，或者确认Job完成的请求失败了，Job将被重新调度到其他Worker去处理。

为了实现此机制，Worker需要周期地回调Redis来确认正在运行的Job，就像是说：“我仍在运行此Job，没有停滞，请不要重试之”。`CheckStalledJobs`方法查找所有沉默了的活跃Job（在`StallInterval`ms周期内没有一次确认正在运行），认定它们已经停滞，发送一个带有Job ID的`stalled`事件，并将它们重新入队以便被其他Worker处理。

## 队列延迟Job激活说明

除非`Queue`的`ActivateDelayedJobs`设置为`true`，否则延迟Job不会被激活。

Job激活的及时性由`Queue`的`DelayedDebounce`设置控制。此设置定义一个窗口，通过该窗口可以对延迟的Job进行分组。如果三个Job的延迟时间分别是10s，10.5s和12s，则当第二个Job的预定时间过去时，1s的`DelayedDebounce`将导致前两个Job被激活。

`Queue`的`NearTermWindow`设置决定了在尝试激活Redis中任何经过的延迟Job之前，`Queue`应等待的最长时间。此设置用于避免在发布`EarlyDelayed`事件的`Queue`发生网络故障时，延迟Job无法被激活。

## 底层实现
每个队列使用以下Redis键:

- `bq:name:id`: (String) 整数型自增ID生成器，决定下一个Job的ID
- `bq:name:jobs`: (Hash) Job ID为键, Job的数据和选项的JOSN字符串为值
- `bq:name:waiting`: (List) 等待处理的Job ID列表
- `bq:name:active`: (List) 正在被处理的Job ID列表
- `bq:name:succeeded`: (Set) 处理成功的Job ID集合
- `bq:name:failed`: (Set) 处理失败的Job ID集合
- `bq:name:delayed`: (Sorted Set) 延迟Job的ID有序集 —— Job ID为键，触发时间戳为分数
- `bq:name:stalling`: (Set) 在当前时间间隔内未“签入”的Job ID的集合
- `bq:name:stallBlock`: (Set) 在当前时间间隔内未“签入”的Job ID的集合
- `bq:name:events`: (Pub/Sub channel) Worker发送Job结果的频道
- `bq:name:earlierDelayed`: (Pub/Sub channel) 当在所有其他Job之前添加新的延迟Job时，创建该Job的脚本将通过该发布/订阅通道发布该Job的时间戳

Bee-Queue不是“轮询式”的，而是由空闲的Worker监听Redis中有Job入队。这是由[BRPOPLPUSH](http://redis.io/commands/BRPOPLPUSH)实现的，也就是将Job从等待列表移到活动列表。Bee-Queue遵循其描述的“可靠队列”模式。

停滞集(stalling set)是从最近的停滞间隔开始以来活动列表的快照。在每个停滞间隔中，Worker都会从停滞集中删除其JobID，因此，在间隔结束时，ID留在停滞集中的所有Job都会错过其窗口（stalled），需要重新运行。运行`CheckStalledJobs`时，它会将停滞集中的所有Job重新入队（进入等待列表），然后对活动列表进行快照并将其存储在停滞集中。

Bee-Queue要求用户自行启动重复地检查停滞集，因为如果我们自动执行，则系统中的每个队列实例都将执行检查。与仅从一个或两个实例进行检查相比，从所有实例进行检查效率较低，并且提供的保证较弱。例如，在10个进程上运行的5000ms的` CheckStalledJobs`间隔平均每500ms进行一次检查，但仅保证每5000ms检查一次。每1000毫秒进行两次检查的实例也将平均每500毫秒进行一次检查，但是在整个时间范围内分布更均匀，并且可以保证每1000毫秒进行一次检查。尽管检查并没有高昂代价，并且经常进行检查也没有什么坏处，但避免不必要的低效方式是该库的重点，因此我们让用户来控制确切地执行检查的进程以及检查频率。

## License

[MIT](https://github.com/elvinchan/gobeeq/blob/master/LICENSE)