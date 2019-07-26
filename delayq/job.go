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
	jobid string `json:"jobid,string"`
	name  string `json:"name,string"`
	topic string `json:"topic,string"`
	data  string `json:"data,string"`

	tryes    int   `json:"tryes,int"`    /*当前尝试第几次，默认是0*/
	addtime  int64 `json:"exectime,int"` /*添加时间*/
	exectime int64 `json:"exectime,int"` /*执行时间*/
	ttr      int64 `json:"ttr,int"`      /*超时时间*/
	state    int   /*任务状态0待处理delay 1进入ready池 2reserve 3.deleted完成后变成删除状态 */
}
type JobList struct {
	jobs *Job
}

type json_resp struct {
	code int         `json:"code,int"`
	info string      `json:"info,string"`
	data interface{} `json:"data,string"`
}

func AddJob(jobid string, name string, topic string, data string, exectime int64) (string, error) {
	job := &Job{
		jobid:    jobid,
		name:     name,
		topic:    topic,
		data:     data,
		exectime: exectime,
		addtime:  time.Now().Unix(),
		ttr:      30,
		state:    STATE_DELAY,
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

	_, err1 := redis_cli.Do("hmset", joblist_key, "jobid", job.jobid, "name", job.name,
		"topic", job.topic, "data", job.data, "addtime", job.addtime, "exectime", job.exectime,
		"ttr", job.ttr, "state", job.state)

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
				jobid:    rk[0],
				name:     rk[1],
				topic:    rk[2],
				data:     rk[3],
				addtime:  utils.String2int64(rk[4]),
				exectime: utils.String2int64(rk[5]),
				ttr:      utils.String2int64(rk[6]),
				state:    utils.String2int(rk[7]),
			}
			nowtime := time.Now().Unix()
			if job.state == STATE_DELAY {
				if job.exectime < nowtime {
					fmt.Println(job.jobid, "达到可执行的时候了，需要丢入ready 池！")
					_, err = redis_cli.Do("hmset", joblist_key_prefix+job.jobid, "state", STATE_READY)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
					_, err = redis_cli.Do("lpush", joblist_ready_pool_key, job.jobid)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
				}
			} else if job.state == STATE_RESERVE {
				elapse := nowtime - job.exectime
				if elapse > job.ttr {
					fmt.Println(job.jobid, "任务在reseave态超时了， 需要重新置入ready态")
					job.exectime = nowtime + job.ttr
					_, err = redis_cli.Do("hmset", joblist_key_prefix+job.jobid, "state", STATE_READY)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
					_, err = redis_cli.Do("lpush", joblist_ready_pool_key, job.jobid)
					if err != nil {
						fmt.Println("redis set failed:", err)
					}
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
		jobid:    rk[0],
		name:     rk[1],
		topic:    rk[2],
		data:     rk[3],
		addtime:  utils.String2int64(rk[4]),
		exectime: utils.String2int64(rk[5]),
		ttr:      utils.String2int64(rk[6]),
		state:    utils.String2int(rk[7]),
	}
	_, err = redis_cli.Do("hmset", joblist_key_prefix+job.jobid, "state", STATE_DELETE)
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
		jobid:    rk[0],
		name:     rk[1],
		topic:    rk[2],
		data:     rk[3],
		addtime:  utils.String2int64(rk[4]),
		exectime: utils.String2int64(rk[5]),
		ttr:      utils.String2int64(rk[6]),
		state:    utils.String2int(rk[7]),
	}
	_, err = redis_cli.Do("hmset", joblist_key_prefix+job.jobid, "state", STATE_RESERVE)
	if err != nil {
		fmt.Println("redis set failed:", err)
		return "", err
	}
	_, err1 := redis_cli.Do("LREM", joblist_ready_pool_key, 0, job.jobid)
	if err1 != nil {
		fmt.Println("redis rem failed:", err)
		return "", errors.New("index fail!")
	}
	json_ret, _ := json.Marshal(job)
	return string(json_ret), nil
}
