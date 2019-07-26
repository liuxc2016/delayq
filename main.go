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

	go func() {
		cmd := cmd.Cmd{}
		cmd.Run()
		dq = delayq.New()
		dq.InitDq()
		dq.Start()
		//ret, err1 := delayq.AddJob("jobs1", "工作1", "testtopic1", "{info:1}", time.Now().Unix())
		//ret, err1 = delayq.AddJob("jobs2", "工作2", "testtopic1", "{info:2}", time.Now().Unix()+600)
		// if err1 != nil {
		// 	fmt.Println(ret, err1)
		// }
	}()
	time.Sleep(time.Second * 10)
}
