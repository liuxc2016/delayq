package delayq

import (
	"fmt"
	"sync"
	"time"

	"../utils"
	"github.com/garyburd/redigo/redis"
)

const (
	STATE_DELAY = iota
	STATE_READY
	STATE_RESERVE
	STATE_DELETE
)

type DelayQ struct {
	config       *utils.Config
	logger       *utils.Logger
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
func New(conf *utils.Config, loger *utils.Logger) *DelayQ {
	once.Do(func() {
		dq = &DelayQ{
			config: conf,
			logger: loger,
		}
		err = dq.InitRedis()
		if err != nil {
			dq.logger.Error(err)
			panic(err)
		}
		dq.scanClose = make(chan bool)
	})
	return dq
}

func (dq *DelayQ) InitRedis() error {
	redis_host := dq.config.Redis.Host
	redis_port := dq.config.Redis.Port
	redis_pass := dq.config.Redis.Password

	// 建立连接池
	dq.pool = &redis.Pool{
		MaxIdle:     dq.config.Redis.MaxIdle, //最大线程
		MaxActive:   dq.config.Redis.MaxActive,
		IdleTimeout: time.Duration(dq.config.Redis.IdleTimeout * int64(time.Second)),
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", redis_host+":"+redis_port,
				redis.DialPassword(redis_pass),
				redis.DialDatabase(dq.config.Redis.Database),
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

//启动
func (dq *DelayQ) Run() {
	go dq.Timer()
}

//结束
func (dq *DelayQ) Stop() {
	dq.scanClose <- true
	return
	//结束进程
}

//扫描JobList,每秒执行一次，
//1.扫瞄delay bucket
//2.扫瞄ready list?
//是否有执行超时的，执行失败的, 丢回到delay pool
//xx--delete--xx是否有消费成功的，消费成功的，移入finishedJobList，（是否有notify_url，由dqclient来处理这个）
func (dq *DelayQ) Timer() {
	defer func() {
		fmt.Println("任务池扫瞄结束!")
		dq.logger.Println("任务池扫瞄结束!")
	}()
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-dq.scanClose:
			fmt.Println("DelayQ收到停止信号，扫瞄中止")
			dq.logger.Println("DelayQ收到停止信号，扫瞄中止")
			return
		case <-tick.C:
			fmt.Println("当前循环时间", time.Now().Format("2006-01-02 15:04:05"))
			str, err := ScanDelayBucket() //扫描delay bucket中的jobid ，到期的丢入ready pool
			if err != nil {
				dq.logger.Println("扫描delay bucket出错" + err.Error() + str)
			}
			str, err = ScanReadyJobs() //扫描ready list
			if err != nil {
				dq.logger.Println("扫描ready pool出错" + err.Error() + str)
			}
		}
	}
}
