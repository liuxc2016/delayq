package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"./consumer"
	"./dqclient"
	"./httpclient"
)

var (
	exitChan chan bool
)

// 信号处理
func handleSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			fmt.Println("捕捉到了信号", sig)
			exitChan <- true
		case syscall.SIGHUP:
			fmt.Println("捕捉到了信号,但是继续执行", sig)
		}
	}()
}

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
	exitChan = make(chan bool, 1)
	/*
	*	开启http服务
	 */
	httpClient := &httpclient.Http{
		Dqclient: dqClient,
	}

	go handleSignal()
	go httpClient.Serve()
	go consumer.Consume("push_url", dqClient)
	for {
		select {
		case <-exitChan:
			fmt.Println("收到结束, 通知client, http结束")
			httpClient.Stop()
			dqClient.Stop()
			time.Sleep(3 * time.Second)
			os.Exit(0)
		}
	}

}
