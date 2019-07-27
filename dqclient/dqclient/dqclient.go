package dqclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"../../delayq"
	"../../utils"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/ini.v1"
)

type DqClient struct {
	cfg          *ini.File
	pool         *redis.Pool
	redis_prefix string
	scanClose    chan bool /*是否结束对joblist的扫描*/
}

var (
	err error
)

func (dqcli *DqClient) InitClient() error {
	redis_host := "47.244.135.251:6379"
	redis_pass := "123456"

	// 建立连接池
	dqcli.pool = &redis.Pool{
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

/*
* 取一个ready态的job出来,置为reserve态
 */
func (dqcli *DqClient) Pop(topic string) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	jobid, err := redis.String(redis_cli.Do("RPOP", delayq.GetRedayPoolKey(topic)))
	if err != nil {
		fmt.Println("rpop返回出错")
		return "", err
	} else {
		fmt.Println("从ready池中获取到jobid", jobid)
	}

	result, err := redis.StringMap(redis_cli.Do("HGETALL", delayq.GetJobKey(jobid)))
	if err != nil {
		return "", err
	}
	if result["topic"] == "" || result["data"] == "" {
		return "", errors.New("Job bucket has expired or is incomplete")
	}
	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_RESERVE)
	if err != nil {
		fmt.Println("redis set STATE_RESERVE failed:", err)
		return "", errors.New("redis set STATE_RESERVE failed:")
	}

	job := &delayq.Job{
		Jobid:    result["jobid"],
		Name:     result["name"],
		Topic:    result["topic"],
		Data:     result["data"],
		Addtime:  utils.String2int64(result["addtime"]),
		Exectime: utils.String2int64(result["exectime"]),
		Tryes:    utils.String2int(result["tryes"]),
		Ttr:      utils.String2int64(result["ttr"]),
		State:    utils.String2int(result["state"]),
	}

	//移入delay pool, 执行时间为当前时间 + ttr ，即可
	job.Exectime = time.Now().Unix() + job.Ttr
	_, err = redis_cli.Do("zadd", delayq.DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
	if err != nil {
		return "", errors.New(job.Jobid + "此任务被消费,丢回jobid到delay bucket 失败！")
	}

	json_ret, _ := json.Marshal(job)
	return string(json_ret), nil
}

/*
* 取一个ready态的job出来--会阻塞
 */
func (dqcli *DqClient) Brpop(topic string, timeout int) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	values, err := redis.String(redis_cli.Do("BRPOP", delayq.GetRedayPoolKey(topic), timeout))
	if err != nil {
		return "", err
	}

	jobid := string(values[1])
	result, err := redis.StringMap(redis_cli.Do("HGETALL", delayq.GetJobKey(jobid)))
	if err != nil {
		return "", err
	}
	if result["topic"] == "" || result["data"] == "" {
		return "", errors.New("Job bucket has expired or is incomplete")
	}
	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_RESERVE)
	if err != nil {
		fmt.Println("redis set STATE_RESERVE failed:", err)
		return "", errors.New("redis set STATE_RESERVE failed:")
	}
	json_ret, _ := json.Marshal(result)
	return string(json_ret), nil

}

/*
* 删除一个Job，必须为joblist中的delay或者ready状态
 */
func (dqcli *DqClient) Remove(jobid string) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_DELETE)
	if err != nil {
		fmt.Println("redis set STATE_DELETE failed:", err)
		return "", errors.New("redis set STATE_RESERVE failed:")
	}

	/*从delay池中删掉*/
	/*将任务id 移出delay bucket*/
	_, err = redis_cli.Do("zrem", delayq.DELAY_BUCKET_KEY, jobid)
	if err != nil {
		return "", errors.New(jobid + "任务从delay_bucket移出失败")
	}

	return "", nil
}
