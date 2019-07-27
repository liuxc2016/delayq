package cmd

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"../delayq"
	"../utils"
)

const (
	APP_VERSION = "1.0.0 test"
)

// 命令类
type Cmd struct {
	dq     *delayq.DelayQ
	exit   chan bool
	conf   string /*如果命令行传入配置文件*/
	daemon bool   /*是否为后台模式*/
	Config utils.Config
	logger utils.Logger
}

// 执行
func (p *Cmd) Run() {
	// 命令行参数处理
	p.exit = make(chan bool)
	// 欢迎
	welcome()

	p.handleFlags()
	// 实例化公共组件

	p.handleSignal()
	fmt.Println(p.conf)
	p.Config = utils.LoadConfig(p.conf)
	fmt.Println(p.Config.Delayq)
	os.Exit(0)
	p.dq = delayq.New()
	p.dq.InitDq()
	p.dq.Start()
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
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			fmt.Println("捕捉到了信号", sig)
			os.Exit(0)
			p.handleSignal()
		}
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
	fmt.Println("Usage: delayer [options]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("-d/--daemon run in the background")
	fmt.Println("-c/--configuration FILENAME -- configuration file path (searches if not given)")
	fmt.Println("-h/--help -- print this usage message and exit")
	fmt.Println("-v/--version -- print version number and exit")
	fmt.Println()
	os.Exit(0)
}

// 打印版本
func printVersion() {
	fmt.Println(APP_VERSION)
	fmt.Println()
	os.Exit(0)
}
