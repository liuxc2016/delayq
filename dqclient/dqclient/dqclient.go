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
	logger       *utils.Logger
	redis_prefix string
	consumClose  chan bool /*是否结束对jobs的消费*/
}

var (
	err error
)

func (dqcli *DqClient) Run() {
	defer func() {
		fmt.Println("消费线程结束!")
		dqcli.logger.Println("消费线程结束!")
	}()
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-dqcli.consumClose:
			fmt.Println("DelayQ收到消费线程结束，中止")
			dqcli.logger.Println("DelayQ收到消费线程结束停止信号，扫瞄中止")
			return
		case <-tick.C:
			fmt.Println("当前循环时间", time.Now().Format("2006-01-02 15:04:05"))
			/*取出任务用来消费*/
		}
	}
}

func (dqcli *DqClient) Stop() {
	fmt.Println("dqclient收到停止信号，结束消费")
	dqcli.consumClose <- true

}

func (dqcli *DqClient) InitClient() error {
	dqcli.consumClose = make(chan bool, 1)
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

	if topic == "" {
		return "", errors.New("topic值不能为空")
	}
	jobid, err := redis.String(redis_cli.Do("RPOP", delayq.GetRedayPoolKey(topic)))
	if err != nil {
		//if err == redis.ErrNil {
		return "", err
	} else {
		fmt.Println("从ready池中获取到jobid", jobid)
	}

	result, err := redis.StringMap(redis_cli.Do("HGETALL", delayq.GetJobKey(jobid)))
	if err != nil {
		return "", err
	}
	if result["topic"] == "" || result["data"] == "" {
		return "", errors.New("从任务信息池中获取到任务信息失败")
	}
	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_RESERVE, "poptime", time.Now().Unix(), "tryes", utils.String2int(result["tryes"])+1)
	if err != nil {
		fmt.Println(jobid, "设置任务信息状态[STATE_RESERVE]失败:", err)
		return "", errors.New("redis 设置任务信息状态[STATE_RESERVE]失败")
	}

	/*将任务丢入reserv bucket*/
	_, err = redis_cli.Do("lpush", delayq.RESERVE_BUCKET_KEY, jobid)
	if err != nil {
		fmt.Println(jobid, "将超时任务丢入[RESERVE_BUCKET]失败:", err)
		return "", errors.New(jobid + "redis 将超时任务丢入[RESERVE_BUCKET]失败")
	}

	job := &delayq.Job{
		Jobid:    result["jobid"],
		Name:     result["name"],
		Topic:    result["topic"],
		Data:     result["data"],
		Addtime:  utils.String2int64(result["addtime"]),
		Poptime:  time.Now().Unix(),
		Exectime: utils.String2int64(result["exectime"]),
		Tryes:    utils.String2int(result["tryes"]) + 1,
		Ttr:      utils.String2int64(result["ttr"]),
		State:    utils.String2int(result["state"]),
	}
	fmt.Println("本次pop请求，client返回：", job)
	json_ret, _ := json.Marshal(job)
	return string(json_ret), nil
}

/*
* 取一个ready态的job出来--会阻塞
 */
func (dqcli *DqClient) Brpop(topic string, timeout int) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	if topic == "" {
		return "", errors.New("topic值不能为空")
	}
	jobid, err := redis.String(redis_cli.Do("BRPOP", delayq.GetRedayPoolKey(topic), timeout))
	if err != nil {
		//if err == redis.ErrNil {
		return "", err
	} else {
		fmt.Println("从ready池中获取到jobid", jobid)
	}

	result, err := redis.StringMap(redis_cli.Do("HGETALL", delayq.GetJobKey(jobid)))
	if err != nil {
		return "", err
	}
	if result["topic"] == "" || result["data"] == "" {
		return "", errors.New("从任务信息池中获取到任务信息失败")
	}
	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_RESERVE)
	if err != nil {
		fmt.Println(jobid, "设置任务信息状态[STATE_RESERVE]失败:", err)
		return "", errors.New("redis 设置任务信息状态[STATE_RESERVE]失败")
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

	json_ret, _ := json.Marshal(job)
	fmt.Println("本次bpop返回,", json_ret)
	return string(json_ret), nil

}

/*
* 完成一个Job，必须为joblist中的delay或者ready状态
 */
func (dqcli *DqClient) FinishJob(jobid string) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	if jobid == "" {
		return "", errors.New("缺少参数jobid:")
	}
	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_FINISH)
	if err != nil {
		fmt.Println("redis set STATE_FINISH failed:", err)
		return "", errors.New("redis set STATE_FINISH failed:")
	}

	/*将任务id 移出reserve bucket, 同时放入finish bucket*/
	_, err = redis_cli.Do("zrem", delayq.DELAY_BUCKET_KEY, jobid)
	if err != nil {
		return "", errors.New(jobid + "任务从delay_bucket移出失败")
	}

	_, err = redis_cli.Do("lpush", delayq.FINISH_BUCKET_KEY, jobid)
	if err != nil {
		return "", errors.New(jobid + "任务添加到[FINISH_BUCKET]失败")
	}
	return "", nil
}

/*
* 标志一个Job为失败，不再执行
 */
func (dqcli *DqClient) FailJob(jobid string) (string, error) {
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	if jobid == "" {
		return "", errors.New("缺少参数jobid:")
	}
	_, err = redis_cli.Do("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_DELETE)
	if err != nil {
		fmt.Println("redis set STATE_DELETE failed:", err)
		return "", errors.New("redis set STATE_DELETE failed:")
	}

	/*将任务id 移出reserve bucket, 同时放入finish bucket*/
	_, err = redis_cli.Do("lrem", delayq.RESERVE_BUCKET_KEY, 0, jobid)
	if err != nil {
		return "", errors.New(jobid + "任务从[RESERVE_BUCKET]移出失败")
	} else {
		fmt.Println(jobid + "任务从[RESERVE_BUCKET]移出成功")
	}

	_, err = redis_cli.Do("lpush", delayq.FAIL_BUCKET_KEY, jobid)
	if err != nil {
		return "", errors.New(jobid + "任务添加到[FAIL_BUCKET_KEY]失败")
	}
	return "", nil
}

/*
*添加一个任务
 */
func (dqcli *DqClient) AddJob(jobid string, name string, topic string, data string, exectime int64, ttr int64) (string, error) {
	if ttr <= 0 {
		ttr = 20
	}
	if jobid == "" || topic == "" {
		return "", errors.New(jobid + topic + "jobid, topic 必须填写！")
	}
	if exectime == 0 {
		exectime = time.Now().Unix() + 1 //如果传入的执行时间为0，表示立即执行
	}
	job := &delayq.Job{
		Jobid:    jobid,
		Name:     name,
		Topic:    topic,
		Data:     data,
		Exectime: exectime,
		Poptime:  0,
		Addtime:  time.Now().Unix(),
		Tryes:    0,
		Ttr:      ttr,
		State:    delayq.STATE_DELAY,
	}
	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	is_key_exit, err := redis.Bool(redis_cli.Do("EXISTS", delayq.GetJobKey(jobid)))
	if err != nil {
		return "", err
	} else {
		if is_key_exit {
			return "", errors.New(jobid + "当前任务id已经存在，不允许重复添加！")
		}
	}

	_, err1 := redis_cli.Do("hmset", delayq.GetJobKey(jobid), "jobid", job.Jobid, "name", job.Name,
		"topic", job.Topic, "data", job.Data, "addtime", job.Addtime, "exectime", job.Exectime, "tryes", job.Tryes,
		"ttr", job.Ttr, "state", job.State)

	if err1 != nil {
		return "", errors.New("添加失败！")
	}

	/*将jobid 丢入delay bucket*/

	_, err1 = redis_cli.Do("zadd", delayq.DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
	if err1 != nil {
		return "", errors.New("添加jobid到delay bucket 失败！")
	}

	job_json, _ := json.Marshal(job)
	return string(job_json), nil
}

/*
*获取一个任务的信息
 */
func (dqcli *DqClient) GetJobInfo(jobid string) (string, error) {

	if jobid == "" {
		return "", errors.New("jobid,  必须填写！")
	}

	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	rk, err := redis.StringMap(redis_cli.Do("HGETALL", delayq.GetJobKey(jobid)))

	if err == nil {
		job := &delayq.Job{
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

		/*发布到top*/
		ret_json, err1 := json.Marshal(job)
		if err1 != nil {
			return "", errors.New(job.Jobid + "任务获取失败，解析失败！")
		}
		if job.Jobid == "" {
			return "", errors.New(job.Jobid + "任务获取失败，任务不存在！")
		}
		return string(ret_json), nil

	} else {
		return "", errors.New(jobid + "获取任务信息失败")
	}
}

/***/

func (dqcli *DqClient) Ping() (string, error) {

	redis_cli := dqcli.pool.Get()
	defer redis_cli.Close()

	pong, err := redis.String(redis_cli.Do("PING"))
	return pong, err
}
