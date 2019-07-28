# DelayQ
## [项目地址](https://github.com/liuxc2016/delayq) 

- 基于 Redis 的延迟消息队列中间件，采用 Golang 开发，支持 PHP、Golang 等多种语言客户端。
- 参考 [有赞延迟队列设计](http://tech.youzan.com/queuing_delay) 中的部分设计，优化后实现。
- 参考 [delayq](https://github.com/mix-basic/delayq) 中的很多设计，优化后实现。

## 应用场景

- 订单超过30分钟未支付，自动关闭订单。
- 订单完成后, 如果用户一直未评价, 5天后自动好评。
- 会员到期前3天，短信通知续费。
- 其他针对某个任务，延迟执行功能的需求。

## 实现原理

- 客户端：push 任务时，任务数据存入 hash 中，jobID 存入 zset 中，pop 时从指定的 list 中取准备好的数据。
- 服务器端：定时使用连接池并行将 zset 中到期的 jobID 放入对应的 list 中，供客户端 pop 取出。

## 核心特征

- 使用 Golang 开发，高性能。
- 高可用：服务器端操作是原子的，并且做了优雅停止，不会丢失数据，在redis断线时会自动重连。
- 可通过配置文件控制执行性能参数。
- 提供多种语言的 SDK，使用简单快捷。

## 整体结构

- 整个延迟队列由4个部分组成：
- Job Pool用来存放所有Job的元信息。
- Delay Bucket是一组以时间为维度的有序队列，用来存放所有需要延迟的／已经被reserve的Job（这里只存放Job Id）。
- Timer负责实时扫描各个Bucket，并将delay时间大于等于当前时间的Job放入到对应的Ready Queue。
- Ready Queue存放处于Ready状态的Job（这里只存放Job Id），以供消费程序消费。

- ![主要流程参考](https://tech.youzan.com/content/images/2016/03/delay-queue.png)

## 如何使用

`delayq` 分为：

- 服务器端：负责定时扫描到期的任务，并放入队列，需在服务器上常驻执行。
- 客户端：在代码中使用，以类库的形式，提供 `push`、`pop`、`bPop`、`remove` 方法操作任务。
- http服务：在代码中使用，以类库的形式，提供 `push`、`pop`、`bPop`、`remove` 方法操作任务。

## 服务器端


go run delayq/main.go
> 支持 windows、linux、mac 三种平台

配置文件 `conf/delayq.conf`：
```
[delayq]
pid = /var/run/delayq.pid      ; 需单例执行时配置, 多实例执行时留空, Win不支持单例
timer_interval = 1000           ; 计算间隔时间, 单位毫秒
access_log = logs/access.log    ; 存取日志
error_log = logs/error.log      ; 错误日志

[redis]
host = 127.0.0.1                ; 连接地址
port = 6379                     ; 连接端口
database = 0                    ; 数据库编号
password =                      ; 密码, 无需密码留空
max_idle = 2                    ; 最大空闲连接数
max_active = 20                 ; 最大激活连接数
idle_timeout = 3600             ; 空闲连接超时时间, 单位秒
conn_max_lifetime = 3600        ; 连接最大生存时间, 单位秒
```

查看帮助：

```
[root@localhost bin]# ./delayq -h
Usage: delayq [options]

Options:
-d/ run in the background
-c/ FILENAME -- configuration file path (searches if not given)
-h -- print this usage message and exit
-v -- print version number and exit
```

启动：

```
D:\goworks\delayq>go run main.go

_____   _____   _           ___  __    __  _____
|  _  \ | ____| | |         /   | \ \  / / /  _  \
| | | | | |__   | |        / /| |  \ \/ /  | | | |
| | | | |  __|  | |       / / | |   \  /   | | | |
| |_| | | |___  | |___   / /  | |   / /    | |_| |_
|_____/ |_____| |_____| /_/   |_|  /_/     \_______|

Service:                delayq
Version:                1.0.0 test
当前循环时间 2019-07-28 23:03:36
delaybucket 为空,本轮扫描结束
scan ready jobs!
ready topics [dqready:top1]
ready job_keys [job1_2 top1_1]
job1_2 正在等待消费， 已超过预计时间 80
捕捉到了信号 interrupt
top1_1 正在等待消费， 已超过预计时间 81
DelayQ收到停止信号，扫瞄中止
任务池扫瞄结束!
```

## 客户端

我们提供了以下几种语言：

```
cd dqclient
go orun dqclient.go
```

### 日志

- 使用logrus构架日志，默认存放在<code>logs/</code>文件夹下
- delayq后端使用日志logs/access.log, 错误日志logs/error.log
- delayq消费端使用日志 logs/topic_access.log, 错误日志logs/topic_error.log

## 尚未完成

1. 定制消费失败任务的重发时间
2. 定制任务的消费次数
3. 提供失败消费任务的redis fail pool
4. 提供成功消费任务的redis finish pool
5. 提供可视化的后台方面显示的joblist 信息
6. 日志的按日分割

7. 将delayq后台 与delayqclient 目录优化，更好的实现隔离性

## License

Apache License Version 2.0, http://www.apache.org/licenses/