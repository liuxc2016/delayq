package consumer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"../../delayq"
	"../../utils"
	"../dqclient"
)

var (
	logger *utils.Logger
)

func init() {
	logger = utils.LogNew("../logs/topic_access.log", "../logs/topic_error.log")
}

/*
*消费话题
 */
func Consume(topic string, dqClient *dqclient.DqClient) {
	for {
		ret_str, err := dqClient.Pop(topic)

		if err != nil {
			//fmt.Println("本次轮询未检查到当前topic的job", topic, ret_str, err)
			//time.Sleep(10 * time.Millisecond)
		} else {
			//fmt.Println(ret_str)
			go DealJob(topic, ret_str)
		}

	}
}

func DealJob(topic string, ret_str string) {
	switch topic {
	case "push_url":
		DealPushUrl(ret_str)
	default:
		return
	}
}

func DealPushUrl(ret_str string) {
	job := &delayq.Job{}

	err := json.Unmarshal([]byte(ret_str), job)
	if err != nil {
		logger.Error(job.Jobid + "处理任务，解析任务失败")
		fmt.Println("处理任务，解析任务失败")
		return
	}
	if job.Data == "" {
		logger.Error(job.Jobid + "处理任务，解析任务失败")
		fmt.Println("处理任务，解析任务失败")
		return
	}
	url := job.Data

	resp, err := http.Get(url)
	if err != nil {
		logger.Error(job.Jobid + "处理任务，发送请求失败" + url + err.Error())
		fmt.Println(job.Jobid, "发送请求失败", url, err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error(job.Jobid + "处理任务，解析url返回失败" + url + err.Error())
		fmt.Println(job.Jobid, "解析url返回失败", url, err)
		return
	}
	fmt.Println(job.Jobid, "解析url返回成功", resp.StatusCode, string(body))

	if resp.StatusCode == 200 {
		dqClient := &dqclient.DqClient{}
		dqClient.InitClient()
		dqClient.FinishJob(job.Jobid)
		logger.Println(job.Jobid, "上一次的状态", job.State, "第", job.Tryes, "次执行，状态码", resp.StatusCode, "任务完成了返回数据", string(body))
	} else {
		logger.Error(job.Jobid, "上一次的状态", job.State, "第", job.Tryes, "次执行，状态码", resp.StatusCode, "处理任务失败了，解析url返回状态码为非200"+url)
	}
}
