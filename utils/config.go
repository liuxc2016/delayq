package utils

import (
	"gopkg.in/ini.v1"
)

type Config struct {
	Delayq    Delayq
	Redis     Redis
	Delayqcli Delayqcli
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

type Delayqcli struct {
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
	delayq := conf.Section("delayq")
	pidFile := delayq.Key("pid_file").String()
	timerInterval, _ := delayq.Key("timer_interval").Int64()
	accessLog := delayq.Key("access_log").String()
	errorLog := delayq.Key("error_log").String()
	redis := conf.Section("redis")
	host := redis.Key("host").String()
	port := redis.Key("port").String()
	database, _ := redis.Key("database").Int()
	password := redis.Key("password").String()
	maxIdle, _ := redis.Key("max_idle").Int()
	maxActive, _ := redis.Key("max_active").Int()
	idleTimeout, _ := redis.Key("idle_timeout").Int64()
	connMaxLifetime, _ := redis.Key("conn_max_lifetime").Int64()

	cli_database, _ := conf.Section("delayqcli").Key("database").Int()
	cli_max_idle, _ := conf.Section("delayqcli").Key("max_idle").Int()
	cli_max_active, _ := conf.Section("delayqcli").Key("max_active").Int()
	cli_idle_timeout, _ := conf.Section("delayqcli").Key("idle_timeout").Int64()
	cli_conn_max_lifetime, _ := conf.Section("delayqcli").Key("conn_max_lifetime").Int64()
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
		Delayqcli: Delayqcli{
			Host:            conf.Section("delayqcli").Key("host").String(),
			Port:            conf.Section("delayqcli").Key("port").String(),
			Database:        cli_database,
			Password:        conf.Section("delayqcli").Key("password").String(),
			MaxIdle:         cli_max_idle,
			MaxActive:       cli_max_active,
			IdleTimeout:     cli_idle_timeout,
			ConnMaxLifetime: cli_conn_max_lifetime,
		},
	}
	return config
}
