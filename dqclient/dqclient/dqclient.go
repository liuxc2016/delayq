package dqclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"../../delayq"
	"../../utils"

	"github.com/garyburd/redigo/redis"
)

type DqClient struct {
	cfg          utils.Config
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
	dqcli.cfg = utils.LoadConfig("../conf/delayq.conf")
	redis_host := dqcli.cfg.Delayqcli.Host
	redis_port := dqcli.cfg.Delayqcli.Port
	redis_pass := dqcli.cfg.Delayqcli.Password

	// 建立连接池
	dqcli.pool = &redis.Pool{
		MaxIdle:   dqcli.cfg.Delayqcli.MaxIdle,   //连接池的最大空闲连接数。设为0表示无限制
		MaxActive: dqcli.cfg.Delayqcli.MaxActive, //1个pool所能分配的最大的连接数目,设为0表示无限制
		// 空闲连接超时时间，超过超时时间的空闲连接会被关闭。
		// 如果设置成0，空闲连接将不会被关闭
		// 应该设置一个比redis服务端超时时间更短的时间
		IdleTimeout: time.Duration(dqcli.cfg.Delayqcli.IdleTimeout) * time.Second,
		// 如果Wait被设置成true，则Get()方法将会阻塞
		Wait: true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", redis_host+":"+redis_port,
				redis.DialPassword(redis_pass),
				redis.DialDatabase(dqcli.cfg.Delayqcli.Database),
				redis.DialConnectTimeout(30*time.Second),
				redis.DialReadTimeout(30*time.Second),
				redis.DialWriteTimeout(30*time.Second))
			if err != nil {
				fmt.Println(err)
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

	redis_cli.Send("MULTI")
	/*设置任务状态为执行中*/
	topic_setting := utils.GetTopicSetting(result["topic"])
	ttr_d := 10
	if utils.String2int(result["tryes"]) < len(topic_setting.Ttr) {
		ttr_d = int(topic_setting.Ttr[utils.String2int(result["tryes"])])
	}

	redis_cli.Send("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_RESERVE, "poptime", time.Now().Unix(), "tryes", utils.String2int(result["tryes"])+1, "ttr", ttr_d)
	/*移出ready 池*/
	redis_cli.Send("lrem", delayq.GetRedayPoolKey(topic), 0, jobid)
	/*移入执行中池*/
	redis_cli.Send("lpush", delayq.RESERVE_BUCKET_KEY, jobid)
	redis_cli.Do("EXEC")
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

	redis_cli.Send("MULTI")
	/*设置任务状态为执行中*/
	redis_cli.Send("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_RESERVE, "poptime", time.Now().Unix(), "tryes", utils.String2int(result["tryes"])+1)
	/*移出ready 池*/
	redis_cli.Send("lrem", delayq.GetRedayPoolKey(topic), 0, jobid)
	/*移入执行中池*/
	redis_cli.Send("lpush", delayq.RESERVE_BUCKET_KEY, jobid)
	redis_cli.Do("EXEC")
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

	redis_cli.Send("MULTI")
	//设置任务状态为完成
	redis_cli.Send("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_FINISH)
	//移出delay_buckt?
	redis_cli.Send("zrem", delayq.DELAY_BUCKET_KEY, jobid)
	//移出reserved pool
	redis_cli.Send("lrem", delayq.RESERVE_BUCKET_KEY, 0, jobid)
	//移入已完成
	redis_cli.Send("sadd", delayq.FINISH_BUCKET_KEY, jobid)
	_, err = redis_cli.Do("EXEC")
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
	redis_cli.Send("MULTI")
	//设置任务状态为完成
	redis_cli.Send("hmset", delayq.GetJobKey(jobid), "state", delayq.STATE_DELETE)
	//移出delay_buckt?
	redis_cli.Send("zrem", delayq.DELAY_BUCKET_KEY, jobid)
	//移出reserved pool
	redis_cli.Send("lrem", delayq.RESERVE_BUCKET_KEY, 0, jobid)
	//移入已完成
	redis_cli.Send("sadd", delayq.FAIL_BUCKET_KEY, jobid)
	_, err = redis_cli.Do("EXEC")
	if err != nil {
		return "", errors.New(jobid + "任务添加到[FINISH_BUCKET]失败")
	}
	return "", nil
}

/*
*添加一个任务
 */
func (dqcli *DqClient) AddJob(jobid string, name string, topic string, data string, exectime int64) (string, error) {

	if jobid == "" || topic == "" {
		return "", errors.New(jobid + topic + "jobid, topic 必须填写！")
	}
	topic_setting := utils.GetTopicSetting(topic)
	ttr := topic_setting.Ttr[0]
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

	redis_cli.Send("MULTI")
	/*添加任务*/

	redis_cli.Send("hmset", delayq.GetJobKey(jobid), "jobid", job.Jobid, "name", job.Name,
		"topic", job.Topic, "data", job.Data, "addtime", job.Addtime, "exectime", job.Exectime, "tryes", job.Tryes,
		"ttr", ttr, "state", job.State)
	/*添加到延时队列*/
	redis_cli.Send("zadd", delayq.DELAY_BUCKET_KEY, job.Exectime, job.Jobid)
	r, err1 := redis_cli.Do("EXEC")
	r = r
	if err1 != nil {
		fmt.Println(err1)
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
