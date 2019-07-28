package main

import (
	"fmt"
	"os"

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
}
