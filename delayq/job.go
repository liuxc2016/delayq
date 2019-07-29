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
	joblist_key string
)

type Job struct {
	Jobid string `json:"jobid"`
	Name  string `json:"name"`
	Topic string `json:"topic"`
	Data  string `json:"data"`

	Addtime  int64 `json:"addtime"`  /*添加时间*/
	Exectime int64 `json:"exectime"` /*执行时间--期望值*/
	Poptime  int64 `json:"poptime"`  /*取出时间，每一次取出重置这个值*/
	Tryes    int   `json:"tryes"`    /*当前尝试第几次，默认是0，每取出一次加1*/

	Ttr   int64 `json:"ttr"`   /*超时时间，由topic配置，每一次取出，使用下一次的topic[tryes]*/
	State int   `json:"state"` /*任务状态0待处理delay 1进入ready池 2reserve 3.deleted完成后变成删除状态 */
}
type JobList struct {
	jobs *Job
}

func GetRedayPoolKey(topic string) string {
	return READY_POOL_KEY_PREFIX + topic
}

func GetJobKey(jobid string) string {
	return JOBLIST_KEY_PREFIX + jobid
}

func AddJob(jobid string, name string, topic string, data string, exectime int64, ttr int64) (string, error) {
	if ttr <= 0 {
		ttr = 20
	}
	if jobid == "" || topic == "" {
		return "", errors.New(jobid + topic + "jobid, topic 必须填写！")
	}
	if exectime == 0 {
		exectime = time.Now().Unix() + 1 //如果传入的执行时间为0，表示立即执行
	}
	job := &Job{
		Jobid:    jobid,
		Name:     name,
		Topic:    topic,
		Data:     data,
		Exectime: exectime,
		Addtime:  time.Now().Unix(),
		Tryes:    0,
		Ttr:      ttr,
		State:    STATE_DELAY,
	}
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	is_key_exit, err := redis.Bool(redis_cli.Do("EXISTS", GetJobKey(jobid)))
	if err != nil {
		return "", err
	} else {
		if is_key_exit {
			return "", errors.New(jobid + "当前任务id已经存在，不允许重复添加！")
		}
	}

	_, err1 := redis_cli.Do("hmset", GetJobKey(jobid), "jobid", job.Jobid, "name", job.Name,
		"topic", job.Topic, "data", job.Data, "addtime", job.Addtime, "exectime", job.Exectime, "tryes", job.Tryes,
		"ttr", job.Ttr, "state", job.State)

	if err1 != nil {
		return "", errors.New("添加失败！")
	}

	/*将jobid 丢入delay bucket*/

	_, err1 = redis_cli.Do("zadd", DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
	if err1 != nil {
		return "", errors.New("添加jobid到delay bucket 失败！")
	}

	job_json, _ := json.Marshal(job)
	return string(job_json), nil
}

/*
*从delay bucket中读出到期的任务id加入ready pool list.
 */
func ScanDelayBucket() (string, error) {
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	fmt.Println("正在扫描[DelayBucket]消费中任务池!")
	now_int := time.Now().Unix()
	job_keys, err := redis.Strings(redis_cli.Do("zrangebyscore", DELAY_BUCKET_KEY, 1, now_int))
	if err != nil {
		return "", err
	}
	if len(job_keys) <= 0 {
		fmt.Println("[DelayBucketScan]本轮为空,本轮扫描结束")
		dq.logger.Println("[DelayBucketScan] 为空,本轮扫描结束")
		return "", nil
	}
	for _, v := range job_keys {
		rk, err := redis.StringMap(redis_cli.Do("HGETALL", GetJobKey(v)))
		fmt.Println(GetJobKey(v), rk)
		if err == nil {
			job := &Job{
				Jobid:    rk["jobid"],
				Name:     rk["name"],
				Topic:    rk["topic"],
				Data:     rk["data"],
				Addtime:  utils.String2int64(rk["addtime"]),
				Exectime: utils.String2int64(rk["exectime"]),
				Tryes:    utils.String2int(rk["tryes"]),
				Ttr:      utils.String2int64(rk["ttr"]),
				State:    utils.String2int(rk["state"]),
			}

			topicSetting := utils.GetTopicSetting(job.Topic)
			fmt.Println(job.Topic, topicSetting)
			if job.Tryes >= topicSetting.MaxTryes {
				/*将任务置为fail状态*/
				_, err = redis_cli.Do("hmset", JOBLIST_KEY_PREFIX+job.Jobid, "state", STATE_DELETE)
				if err != nil {
					dq.logger.Println("[DelayBucketScan] ", job.Jobid+"任务置为fail 状态失败")
					return "", errors.New(job.Jobid + "任务置为fail 状态失败")
				}

				/*将任务id 移出delay bucket*/
				_, err = redis_cli.Do("zrem", DELAY_BUCKET_KEY, job.Jobid)
				if err != nil {
					dq.logger.Println("[DelayBucketScan] ", job.Jobid+"移出delay bucket失败")
					return "", errors.New(job.Jobid + "移出delay bucket移出失败")
				}

				/*将任务加到fail bucket*/
				_, err = redis_cli.Do("lpush", FAIL_BUCKET_KEY, job.Jobid)
				if err != nil {
					dq.logger.Println("[DelayBucketScan] ", job.Jobid+"添加到[FAIL_BUCKET]失败")
					return "", errors.New(job.Jobid + "添加到[FAIL_BUCKET]失败")
				}
			} else {
				/*将任务置为ready状态*/
				_, err = redis_cli.Do("hmset", JOBLIST_KEY_PREFIX+job.Jobid, "state", STATE_READY)
				if err != nil {
					dq.logger.Println("[DelayBucketScan] ", job.Jobid+"任务置为ready 状态失败")
					return "", errors.New(job.Jobid + "任务置为ready 状态失败")
				}
				/*将任务丢入到ready pool */
				_, err = redis_cli.Do("lpush", GetRedayPoolKey(job.Topic), job.Jobid)
				if err != nil {
					dq.logger.Println("[DelayBucketScan] ", job.Jobid+"任务丢入ready pool失败")
					return "", errors.New(job.Jobid + "任务丢入ready pool失败")
				} else {
					fmt.Println(job.Jobid+"任务丢入ready pool成功", job.Tryes)
				}

				/*将任务id 移出delay bucket*/
				_, err = redis_cli.Do("zrem", DELAY_BUCKET_KEY, job.Jobid)
				if err != nil {
					dq.logger.Println("[DelayBucketScan] ", job.Jobid+"任务丢入ready pool失败")
					return "", errors.New(job.Jobid + "任务从delay_bucket移出失败")
				}

				/*发布到top*/
				ret_json, err1 := json.Marshal(job)
				if err1 != nil {
					return "", errors.New(job.Jobid + "任务发布失败")
				}
				_, err = redis_cli.Do("Publish", job.Topic, ret_json)
				if err != nil {
					return "", errors.New(job.Jobid + "任务发布失败")
				} else {
					return "", nil
				}
			}
		} else {
			fmt.Println("获取任务状态失败hmget err", err)
			return "", errors.New(v + "获取任务状态失败")
		}
	}
	return "", nil

}

/**
*从reserve bucket中读出所有的任务，将超时的任务id加入delay pool.
 */
func ScanReserveBucket() (string, error) {
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	fmt.Println("正在扫描[ReserveBucket]消费中任务池!")

	job_keys, err1 := redis.Strings(redis_cli.Do("lrange", RESERVE_BUCKET_KEY, 0, -1))
	fmt.Println("[ReserveBucket]当前消费中的任务", job_keys)
	if err1 != nil {
		return "", err1
	}
	if len(job_keys) <= 0 {
		return "", nil
	}

	for _, v := range job_keys {
		//rk, err := redis.Strings(redis_cli.Do("hmget", v, "jobid", "name", "topic", "data", "addtime", "exectime", "ttr", "state"))
		rk, err := redis.StringMap(redis_cli.Do("HGETALL", GetJobKey(v)))

		if err == nil {
			job := &Job{
				Jobid:    rk["jobid"],
				Name:     rk["name"],
				Topic:    rk["topic"],
				Data:     rk["data"],
				Addtime:  utils.String2int64(rk["addtime"]),
				Exectime: utils.String2int64(rk["exectime"]),
				Tryes:    utils.String2int(rk["tryes"]),
				Ttr:      utils.String2int64(rk["ttr"]),
				State:    utils.String2int(rk["state"]),
			}
			nowtime := time.Now().Unix()

			elapse := nowtime - job.Poptime
			if elapse > job.Ttr {
				fmt.Println(job.Jobid, "任务在[Reserve]消费状态超时了， ", job.Ttr, "秒以后重新执行")
				dq.logger.Println(job.Jobid, "任务在[Reserve]消费状态超时了， ", job.Ttr, "秒以后重新执行")
				//
				job.Exectime = nowtime + job.Ttr

				job.Tryes += 1

				/*修改任务状态*/
				_, err = redis_cli.Do("hmset", GetJobKey(job.Jobid), "state", STATE_DELAY, "tryes", job.Tryes, "exectime", job.Exectime)
				if err != nil {
					fmt.Println("redis set failed:", err)
					return "", errors.New(job.Jobid + "此任务超时，重置任务为delay态失败！")
				} else {
					fmt.Println(job.Jobid + "此任务超时，重置任务为delay态成功！")
				}

				/*丢回delay bucket*/
				_, err1 = redis_cli.Do("zadd", DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
				if err1 != nil {
					fmt.Println(job.Jobid + "此任务超时,丢回jobid到delay bucket 失败！")
					return "", errors.New(job.Jobid + "此任务超时,丢回jobid到delay bucket 失败！")
				} else {
					fmt.Println(job.Jobid + "此任务超时,丢回jobid到delay bucket 丢回！")
				}

				/*
				*从reserve中移出
				 */
				_, err = redis_cli.Do("lrem", RESERVE_BUCKET_KEY, 0, job.Jobid)
				if err != nil {
					fmt.Println(job.Jobid, "移出失败", err)
					return "", errors.New(job.Jobid + "此任务超时，移出[RESERVE_BUCKET]池失败！")
				} else {
					dq.logger.Println(job.Jobid, "任务已被移出[RESERVE_BUCKET]", job.Topic)
					fmt.Println("任务已被移出[RESERVE_BUCKET]", job.Jobid, job.Topic)
				}

			} else {
				dq.logger.Error(job.Jobid, "正在等待消费， 已超过预计时间", elapse)
				fmt.Println(job.Jobid, "正在等待消费， 已超过预计时间", elapse)
			}

		} else {
			return "", err
		}
	}

	return "", nil
}

/**
*从reserve pool中读出所有的任务，将超时的任务id加入delay pool.
 */
func ScanReserveJobs() (string, error) {
	redis_cli := dq.pool.Get()
	defer redis_cli.Close()

	job_keys, err := redis.Strings(redis_cli.Do("lrange", RESERVE_BUCKET_KEY, 0, -1))

	if err != nil {
		return "", err
	}
	if len(job_keys) <= 0 {
		return "", nil
	}
	for _, v := range job_keys {
		rk, err := redis.StringMap(redis_cli.Do("HGETALL", GetJobKey(v)))
		fmt.Println(GetJobKey(v), rk)
		if err == nil {
			job := &Job{
				Jobid:    rk["jobid"],
				Name:     rk["name"],
				Topic:    rk["topic"],
				Data:     rk["data"],
				Addtime:  utils.String2int64(rk["addtime"]),
				Exectime: utils.String2int64(rk["exectime"]),
				Tryes:    utils.String2int(rk["tryes"]),
				Ttr:      utils.String2int64(rk["ttr"]),
				State:    utils.String2int(rk["state"]),
			}

			nowtime := time.Now().Unix()
			elapse := nowtime - job.Poptime
			if elapse > job.Ttr {
				topic_setting := utils.GetTopicSetting(job.Topic)
				if job.Tryes >= topic_setting.MaxTryes {
					/*丢到[FAIL_BUCKET_KEY]失败任务池里*/

					/*1.修改任务状态*/
					_, err = redis_cli.Do("hmset", GetJobKey(job.Jobid), "state", STATE_DELETE)
					if err != nil {
						fmt.Println(job.Jobid + "此任务被取出消费，但执行超时失败！")
						dq.logger.Error(job.Jobid + "此任务被取出消费，但执行超时失败！")
						return "", errors.New(job.Jobid + "此任务被取出消费，但执行超时失败！")
					}

					_, err = redis_cli.Do("lpush", FAIL_BUCKET_KEY, job.Jobid)
					if err != nil {
						dq.logger.Println(job.Jobid + "此任务被取出消费且执行超时失败,未成功失败任务池失败！")
						return "", errors.New(job.Jobid + "此任务被取出消费且执行超时失败,未成功失败任务池失败！")
					}

				} else {
					/*丢回delaybucket里，等待下次执行*/

					job.Exectime = nowtime + topic_setting.Ttr[job.Tryes]
					fmt.Println(job.Jobid, "任务在reseave态超时失败， 将重新执行，当前为第", job.Tryes, "次，下次执行将在秒后：", topic_setting.Ttr[job.Tryes])

					/*修改任务状态*/
					_, err = redis_cli.Do("hmset", GetJobKey(job.Jobid), "state", STATE_DELAY, "tryes", job.Tryes, "exectime", job.Exectime)
					if err != nil {
						fmt.Println(job.Jobid+"此任务第", job.Tryes, "次超时，重置任务为delay态失败！")
						dq.logger.Error(job.Jobid+"此任务第", job.Tryes, "次超时，重置任务为delay态失败！")
						return "", errors.New(job.Jobid + "此任务超时，重置任务为delay态失败！")
					}
					job.Tryes += 1

					/*丢回delay bucket*/
					_, err = redis_cli.Do("zadd", DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
					if err != nil {
						return "", errors.New(job.Jobid + "此任务超时,丢回jobid到delay bucket 失败！")
					}

					/*
					*从ready list中移出
					 */
					_, err = redis_cli.Do("lrem", RESERVE_BUCKET_KEY, 0, job.Jobid)
					if err != nil {
						fmt.Println(job.Jobid+"移出[RESERVE_BUCKET]失败", err)
						dq.logger.Error(job.Jobid+"移出[RESERVE_BUCKET]失败", err)
						return "", errors.New(job.Jobid + "此任务超时，移出[RESERVE_BUCKET]池失败！")
					}
					//fmt.Println("移出", RESERVE_BUCKET_KEY, job.Jobid)
				}
			} else {
				fmt.Println(job.Jobid, "正在消费中， 已消费耗时", elapse, "/", job.Ttr)
			}

		} else {
			fmt.Println(v+"获取任务状态失败", err)
			return "", errors.New(v + "获取任务状态失败")
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
	_, err = redis_cli.Do("hmset", GetJobKey(jobid), "state", STATE_DELETE)
	if err != nil {
		fmt.Println("结束任务出错", err)
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
	rk, err := redis.StringMap(redis_cli.Do("HGETALL", GetJobKey(jobid)))
	if err != nil {
		return "", err
	}
	job := &Job{
		Jobid:    rk["jobid"],
		Name:     rk["name"],
		Topic:    rk["topic"],
		Data:     rk["data"],
		Addtime:  utils.String2int64(rk["addtime"]),
		Exectime: utils.String2int64(rk["exectime"]),
		Tryes:    utils.String2int(rk["tryes"]),
		Ttr:      utils.String2int64(rk["ttr"]),
		State:    utils.String2int(rk["state"]),
	}
	_, err = redis_cli.Do("hmset", JOBLIST_KEY_PREFIX+job.Jobid, "state", STATE_RESERVE)
	if err != nil {
		fmt.Println("redis set failed:", err)
		return "", err
	}
	_, err1 := redis_cli.Do("LREM", GetRedayPoolKey(job.Topic), 0, job.Jobid)
	if err1 != nil {
		fmt.Println("redis rem failed:", err)
		return "", errors.New("index fail!")
	}

	//移入delay pool, 执行时间为当前时间 + ttr ，即可
	job.Exectime = time.Now().Unix() + job.Ttr
	_, err = redis_cli.Do("zadd", DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
	if err != nil {
		return "", errors.New(job.Jobid + "此任务被消费,丢回jobid到delay bucket 失败！")
	}

	json_ret, _ := json.Marshal(job)
	return string(json_ret), nil
}
