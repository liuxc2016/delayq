package delayq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"../utils"

	"github.com/garyburd/redigo/redis"
)

var (
	joblist_key_prefix     string = "dqjobs:"
	joblist_key            string
	joblist_ready_pool_key string = "dqready"
)

type Job struct {
	Jobid string `json:"jobid"`
	Name  string `json:"name"`
	Topic string `json:"topic"`
	Data  string `json:"data"`

	Tryes    int   `json:"tryes"`    /*当前尝试第几次，默认是0*/
	Addtime  int64 `json:"exectime"` /*添加时间*/
	Exectime int64 `json:"exectime"` /*执行时间*/
	Ttr      int64 `json:"ttr"`      /*超时时间*/
	State    int   /*任务状态0待处理delay 1进入ready池 2reserve 3.deleted完成后变成删除状态 */
}
type JobList struct {
	jobs *Job
}

func AddJob(jobid string, name string, topic string, data string, exectime int64) (string, error) {
	job := &Job{
		Jobid:    jobid,
		Name:     name,
		Topic:    topic,
		Data:     data,
		Exectime: exectime,
		Addtime:  time.Now().Unix(),
		Ttr:      30,
		State:    STATE_DELAY,
	}
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	joblist_key = joblist_key_prefix + jobid

	is_key_exit, err := redis.Bool(redis_cli.Do("EXISTS", joblist_key))
	if err != nil {
		return "", err
	} else {
		if is_key_exit {
			return "", errors.New("当前任务id已经存在，不允许重复添加！")
		}
	}

	_, err1 := redis_cli.Do("hmset", joblist_key, "jobid", job.Jobid, "name", job.Name,
		"topic", job.Topic, "data", job.Data, "addtime", job.Addtime, "exectime", job.Exectime,
		"ttr", job.Ttr, "state", job.State)

	if err1 != nil {
		return "", errors.New("添加失败！")
	}

	job_json, _ := json.Marshal(job)
	return string(job_json), nil
}

/**
*从redis中读出所有的任务，将到期的任务id加入ready pool.
 */
func CheckJobList() (string, error) {
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	fmt.Println("scan!")
	job_keys, err := redis.Strings(redis_cli.Do("KEYS", joblist_key_prefix+"*"))
	if err != nil {

		return "", err
	}
	for _, v := range job_keys {
		rk, err := redis.Strings(redis_cli.Do("hmget", v, "jobid", "name", "topic", "data", "addtime", "exectime", "ttr", "state"))
		if err == nil {
			job := &Job{
				Jobid:    rk[0],
				Name:     rk[1],
				Topic:    rk[2],
				Data:     rk[3],
				Addtime:  utils.String2int64(rk[4]),
				Exectime: utils.String2int64(rk[5]),
				Ttr:      utils.String2int64(rk[6]),
				State:    utils.String2int(rk[7]),
			}
			nowtime := time.Now().Unix()
			if job.State == STATE_DELAY {
				if job.Exectime < nowtime {
					fmt.Println(job.Jobid, "达到可执行的时候了，需要丢入ready 池！")
					_, err = redis_cli.Do("hmset", joblist_key_prefix+job.Jobid, "state", STATE_READY)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
					_, err = redis_cli.Do("lpush", joblist_ready_pool_key, job.Jobid)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
				}
			} else if job.State == STATE_RESERVE {
				elapse := nowtime - job.Exectime
				if elapse > job.Ttr {
					fmt.Println(job.Jobid, "任务在reseave态超时了， 需要重新置入ready态")
					job.Exectime = nowtime + job.Ttr
					_, err = redis_cli.Do("hmset", joblist_key_prefix+job.Jobid, "state", STATE_READY)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
					_, err = redis_cli.Do("lpush", joblist_ready_pool_key, job.Jobid)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}

				}
			} else if job.State == STATE_READY {
				ret_json, err1 := json.Marshal(job)
				if err1 != nil {
					panic(err1)
				}
				_, err = redis_cli.Do("Publish", job.Topic, ret_json)
				if err != nil {
					fmt.Println("发布失败")
				} else {
					fmt.Println("发布成功", job.Topic, string(ret_json))
				}
			}
		} else {
			fmt.Println("hmget err", err)
			return "", err
		}
	}

	return "", nil
}

/*
* 完成一个job
 */
func FinishJob(jobid string) (string, error) {
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()
	fmt.Println(jobid, "任务已经结束")
	rk, err := redis.Strings(redis_cli.Do("hmget", joblist_key_prefix+jobid, "jobid", "name", "topic", "data", "addtime", "exectime", "ttr", "state"))
	if err != nil {
		return "", err
	}
	job := &Job{
		Jobid:    rk[0],
		Name:     rk[1],
		Topic:    rk[2],
		Data:     rk[3],
		Addtime:  utils.String2int64(rk[4]),
		Exectime: utils.String2int64(rk[5]),
		Ttr:      utils.String2int64(rk[6]),
		State:    utils.String2int(rk[7]),
	}
	_, err = redis_cli.Do("hmset", joblist_key_prefix+job.Jobid, "state", STATE_DELETE)
	if err != nil {
		fmt.Println("delete job faild failed:", err)
		return "", err
	}
	return "", nil
}

/*
* 消费一个job
 */
func ConsumerJob(jobid string) (string, error) {
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()
	rk, err := redis.Strings(redis_cli.Do("hmget", joblist_key_prefix+jobid, "jobid", "name", "topic", "data", "addtime", "exectime", "ttr", "state"))
	if err != nil {
		return "", err
	}
	job := &Job{
		Jobid:    rk[0],
		Name:     rk[1],
		Topic:    rk[2],
		Data:     rk[3],
		Addtime:  utils.String2int64(rk[4]),
		Exectime: utils.String2int64(rk[5]),
		Ttr:      utils.String2int64(rk[6]),
		State:    utils.String2int(rk[7]),
	}
	_, err = redis_cli.Do("hmset", joblist_key_prefix+job.Jobid, "state", STATE_RESERVE)
	if err != nil {
		fmt.Println("redis set failed:", err)
		return "", err
	}
	_, err1 := redis_cli.Do("LREM", joblist_ready_pool_key, 0, job.Jobid)
	if err1 != nil {
		fmt.Println("redis rem failed:", err)
		return "", errors.New("index fail!")
	}
	json_ret, _ := json.Marshal(job)
	return string(json_ret), nil
}
