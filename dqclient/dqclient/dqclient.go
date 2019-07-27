package dqclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
	dqcli.redis_prefix = "delayq:"
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
	json_ret, _ := json.Marshal(result)
	return string(json_ret), nil
}

/*
* 取一个ready态的job出来--会阻塞
 */
func (dqcli *DqClient) Brpop(topic string, timeout int) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	values, err := redis.String(redis_cli.Do("RRPOP", delayq.GetRedayPoolKey(topic), timeout))
	if err != nil {
		return "", err
	}

	jobid := values[1]
	result, err := redis.StringMap(p.Conn.Do("HGETALL", delayq.GetJobKey(jobid)))
	if err != nil {
		return nil, err
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
func (dqcli *DqClient) Remove() {

}
