package delayq

import (
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/ini.v1"
)

const (
	STATE_DELAY = iota
	STATE_READY
	STATE_RESERVE
	STATE_DELETE
)

type DelayQ struct {
	cfg          *ini.File
	pool         *redis.Pool
	redis_prefix string
	scanClose    chan bool /*是否结束对joblist的扫描*/
}

var (
	dq   *DelayQ
	once sync.Once
	err  error
)

/*

 */
func New() *DelayQ {
	once.Do(func() {
		dq = &DelayQ{}
	})
	return dq
}

func (dq *DelayQ) InitRedis() error {
	dq.redis_prefix = "delayq:"
	redis_host := "47.244.135.251:6379"
	redis_pass := "123456"

	// 建立连接池
	dq.pool = &redis.Pool{
		MaxIdle:     5, //最大线程
		MaxActive:   5,
		IdleTimeout: 30 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", redis_host,
				redis.DialPassword(redis_pass),
				redis.DialDatabase(6),
				redis.DialConnectTimeout(30*time.Second),
				redis.DialReadTimeout(30*time.Second),
				redis.DialWriteTimeout(30*time.Second))
			if err != nil {
				return nil, err
			}
			return con, nil
		},
	}
	return err
}

func (dq *DelayQ) InitDq() {
	err = dq.InitRedis()
	if err != nil {
		panic(err)
	}
	dq.scanClose = make(chan bool)
}

//启动
func (dq *DelayQ) Start() {
	//
	go dq.Scan()
}

//结束
func (dq *DelayQ) Stop() {
	//是否写入数据库？
	//结束进程
}

//扫描JobList,每秒执行一次，
//1.扫瞄delay bucket
//2.扫瞄ready list?
//是否有执行超时的，或是执行失败的,检查任务是第几次执行，
//xx--delete--xx是否有消费成功的，消费成功的，移入finishedJobList，（是否有notify_url，由dqclient来处理这个）
func (dq *DelayQ) Scan() {
	defer func() {
		fmt.Println("任务池扫瞄结束!")
	}()
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-dq.scanClose:
			return
		case <-tick.C:
			fmt.Println("当前循环时间", time.Now().Format("2006-01-02 15:04:05"))
			ScanDelayBucket() //扫描delay bucket中的jobid ，到期的丢入ready pool
			ScanReadyJobs()   //扫描ready list
		}
	}
}

/*
* 订阅一个topic
 */
func (dq *DelayQ) Subjob2() {

	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	fmt.Println("订阅一个topic, testtopic3!")

	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-tick.C:
			fmt.Println("当前循环时间", time.Now().Format("2006-01-02 15:04:05"))
		}
	}
}
