package cmd

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"../delayq"
)

const (
	APP_VERSION = "1.0.0 test"
)

// 命令类
type Cmd struct {
	dq   *delayq.DelayQ
	exit chan bool
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
			p.handleSignal()
		}
	}()
}

// 参数处理
func (p *Cmd) handleFlags() (bool, string) {
	// 参数解析
	flagD := flag.Bool("d", false, "")
	flagDaemon := flag.Bool("daemon", false, "")
	flagH := flag.Bool("h", false, "")
	flagHelp := flag.Bool("help", false, "")
	flagV := flag.Bool("v", false, "")
	flagVersion := flag.Bool("version", false, "")
	flagC := flag.String("c", "", "")
	flagConfiguration := flag.String("configuration", "", "")
	flag.Parse()
	// 参数取值
	daemon := *flagD || *flagDaemon
	help := *flagH || *flagHelp
	version := *flagV || *flagVersion
	configuration := ""
	if *flagC == "" {
		configuration = *flagConfiguration
	} else {
		configuration = *flagC
	}
	// 打印型命令处理
	if help {
		printHelp()
	}
	if version {
		printVersion()
	}
	// 返回参数值
	return daemon, configuration
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
