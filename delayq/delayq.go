package delayq

import (
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/ini.v1"
)

const (
	STATE_DELAY   = 0
	STATE_READY   = 1
	STATE_RESERVE = 2
	STATE_DELETE  = 3
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

func (*DelayQ) InitConf() error {
	dq.cfg, err = ini.Load("conf/delayq.conf")
	if err != nil {
		return err
	}
	pid := dq.cfg.Section("delayq").Key("pid").String()
	fmt.Println(pid)
	return nil
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
	err = dq.InitConf()
	if err != nil {
		panic(err)
	}
	err = dq.InitRedis()
	if err != nil {
		panic(err)
	}
	dq.scanClose = make(chan bool)
}

//启动
func (dq *DelayQ) Start() {
	go dq.Scanjob()
	//从redis中读取joblist
}

//结束
func (dq *DelayQ) Stop() {
	//是否写入数据库？
	//结束进程
}

//扫描JobList,每秒执行一次，
//将ready态的任务丢到ready pool
//检查reserved任务，
//是否有超时的或是执行失败的,检查任务是第几次执行，
//是否有消费成功的，消费成功的，移入finishedJobList，是否有notify_url?
func (dq *DelayQ) Scanjob() {
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
			CheckJobList()
		}
	}
}

//定时取出ready job以供消费,给job打上reserved状态
func (dq *DelayQ) doJob() {

}
