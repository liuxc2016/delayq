package main

import (
	"fmt"
	"os"

	"./dqclient"
	"./httpclient"
)

var (
	exitChan chan bool
)

/**
*dqclient --delayq的客户模块
*提交添加job
*添加topic??
*消费job
*完成job
 */

func main() {

	//完成job，提交给dq--说明任务已经完成

	dqClient := &dqclient.DqClient{}
	dqClient.InitClient()

	/*
	*	开启http服务
	 */
	httpClient := &httpclient.Http{
		Dqclient: dqClient,
	}
	go httpClient.Serve()

	for {
		select {
		case <-exitChan:
			fmt.Println("收到结束")
			os.Exit(0)
		}
	}

	// ret, err := dqclient.Pop("testtopic3")
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	job := delayq.Job{}
	// 	err := json.Unmarshal([]byte(ret), &job)
	// 	if err != nil {
	// 		fmt.Println("解析失败", err)
	// 	} else {
	// 		fmt.Println("解析成功", ret)
	// 	}
	// }

	// ret, err = dqclient.Remove("jobs3")
	// if err != nil {
	// 	fmt.Println("删除任务失败", err)
	// } else {
	// 	fmt.Println("删除任务成功", ret)
	// }
}
