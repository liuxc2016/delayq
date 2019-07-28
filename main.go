package main

import (
	"fmt"
	"os"
	"time"

	"./cmd"
	"./delayq"
)

var (
	dq  *delayq.DelayQ
	err error
)

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
}
func main() {

	cmd := cmd.Cmd{}
	cmd.Run()
	//test()
	time.Sleep(time.Second * 500)
}

func test() {
	ret, err1 := delayq.AddJob("jobs1", "工作1", "testtopic1", "{info:1}", time.Now().Unix(), 30)
	if err1 != nil {
		fmt.Println("添加成功job1", ret, err1)
	}
	ret, err1 = delayq.AddJob("jobs2", "工作2", "testtopic1", "{info:2}", time.Now().Unix()+40, 30)
	if err1 != nil {
		fmt.Println("添加成功job2", ret, err1)
	}

	time.AfterFunc(4, func() {
		fmt.Println("i m after func!")
		_, _ = delayq.AddJob("jobs3", "工作3", "testtopic3", "{info:3}", time.Now().Unix()+4, 30)
	})
}
