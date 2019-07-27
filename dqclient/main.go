package main

import (
	"encoding/json"
	"fmt"

	"../delayq"
	"./dqclient"
)

/**
*dqclient --delayq的客户模块
*提交添加job
*添加topic??
*消费job
*完成job
 */

func main() {

	//1订阅topic--list

	//2针对不同的topic，进行不同的处理

	//完成job，提交给dq--说明任务已经完成

	dqclient := &dqclient.DqClient{}
	dqclient.InitClient()
	ret, err := dqclient.Pop("testtopic3")
	if err != nil {
		fmt.Println(err)
	} else {
		job := delayq.Job{}
		err := json.Unmarshal([]byte(ret), &job)
		if err != nil {
			fmt.Println("解析失败", err)
		} else {
			fmt.Println("解析成功", ret)
		}
	}

	ret, err = dqclient.Remove("jobs3")
	if err != nil {
		fmt.Println("删除任务失败", err)
	} else {
		fmt.Println("删除任务成功", ret)
	}
}
