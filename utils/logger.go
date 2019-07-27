package utils

import (
	log "github.com/Sirupsen/logrus"
)

type Logger struct {
	AccessLog *log.Logger
	ErrorLog  *log.Logger
}

func InitLog(logger *Logger) {
	logger.AccessLog = log.New()
	logger.ErrorLog = log.New()

}
