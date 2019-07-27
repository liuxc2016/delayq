package main

import (
	"fmt"

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
	ret, err := dqclient.Brpop("testtopic1", 10)
	fmt.Println(ret, err)

}
