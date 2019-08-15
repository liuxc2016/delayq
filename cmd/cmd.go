package cmd

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"../delayq"
	"../utils"
)

const (
	APP_VERSION = "1.0.0 test"
)

// 命令类
type Cmd struct {
	dq       *delayq.DelayQ
	exitChan chan bool
	conf     string /*如果命令行传入配置文件*/
	daemon   bool   /*是否为后台模式*/
	Config   utils.Config
	logger   *utils.Logger
}

// 执行
func (p *Cmd) Run() {
	// 命令行参数处理
	p.exitChan = make(chan bool)
	// 欢迎
	welcome()

	p.handleFlags()
	// 实例化公共组件

	p.handleSignal()

	p.Config = utils.LoadConfig(p.conf)
	p.logger = utils.LogNew(p.Config.Delayq.AccessLog, p.Config.Delayq.ErrorLog)
	p.dq = delayq.New(&p.Config, p.logger)

	p.dq.Run()

	for {
		select {
		case <-p.exitChan:
			return
		}
	}
}

// 欢迎信息
func welcome() {
	logo := `
_____   _____   _           ___  __    __  _____    
|  _  \ | ____| | |         /   | \ \  / / /  _  \   
| | | | | |__   | |        / /| |  \ \/ /  | | | |   
| | | | |  __|  | |       / / | |   \  /   | | | |   
| |_| | | |___  | |___   / /  | |   / /    | |_| |_  
|_____/ |_____| |_____| /_/   |_|  /_/     \_______| 
	`
	fmt.Println(logo)
	fmt.Println("Service:		delayq")
	fmt.Println("Version:		" + APP_VERSION)
}

// 信号处理
func (p *Cmd) handleSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			fmt.Println("捕捉到了信号", sig)
			p.dq.Stop()
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}
	case case syscall.SIGHUP:
		fmt.Println("捕捉到了信号,但继续执行", sig)
	}()
}

// 参数处理
func (p *Cmd) handleFlags() {
	// 参数解析
	flagD := flag.Bool("d", false, "")

	flagH := flag.Bool("h", false, "")

	flagV := flag.Bool("v", false, "")

	flagC := flag.String("c", "", "")

	flag.Parse()
	// 参数取值
	p.daemon = *flagD
	help := *flagH
	version := *flagV
	p.conf = *flagC
	// 打印型命令处理
	if help {
		printHelp()
	}
	if version {
		printVersion()

	}
	// 返回参数值
	return
}

// 打印帮助
func printHelp() {
	fmt.Println("Usage: delayq [options]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("-d run in the background")
	fmt.Println("-c FILENAME -- configuration file path (searches if not given)")
	fmt.Println("-h -- print this usage message and exit")
	fmt.Println("-v -- print version number and exit")
	fmt.Println()
	os.Exit(0)
}

// 打印版本
func printVersion() {
	fmt.Println(APP_VERSION)
	fmt.Println()
	os.Exit(0)
}
