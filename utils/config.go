package utils

import (
	"fmt"

	"gopkg.in/ini.v1"
)

type Config struct {
	Delayq Delayq
	Redis  Redis
}

type Delayq struct {
	PidFile       string
	TimerInterval int64
	AccessLog     string
	ErrorLog      string
}

type Redis struct {
	Host            string
	Port            string
	Database        int
	Password        string
	MaxIdle         int
	MaxActive       int
	IdleTimeout     int64
	ConnMaxLifetime int64
}

func LoadConfig(conf_file string) Config {
	if conf_file == "" {
		conf_file = "./conf/delayq.conf"
	}
	conf, err := ini.Load(conf_file)
	if err != nil {
		panic("读取配置文件失败" + conf_file)
	}
	// 提取数据
	delayer := conf.Section("delayer")
	pidFile := delayer.Key("pid_file").String()
	fmt.Println("pidFile", pidFile)
	timerInterval, _ := delayer.Key("timer_interval").Int64()
	accessLog := delayer.Key("access_log").String()
	errorLog := delayer.Key("error_log").String()
	redis := conf.Section("redis")
	host := redis.Key("host").String()
	port := redis.Key("port").String()
	database, _ := redis.Key("database").Int()
	password := redis.Key("password").String()
	maxIdle, _ := redis.Key("max_idle").Int()
	maxActive, _ := redis.Key("max_active").Int()
	idleTimeout, _ := redis.Key("idle_timeout").Int64()
	connMaxLifetime, _ := redis.Key("conn_max_lifetime").Int64()
	// 返回
	config := Config{
		Delayq: Delayq{
			PidFile:       pidFile,
			TimerInterval: timerInterval,
			AccessLog:     accessLog,
			ErrorLog:      errorLog,
		},
		Redis: Redis{
			Host:            host,
			Port:            port,
			Database:        database,
			Password:        password,
			MaxIdle:         maxIdle,
			MaxActive:       maxActive,
			IdleTimeout:     idleTimeout,
			ConnMaxLifetime: connMaxLifetime,
		},
	}
	return config
}
