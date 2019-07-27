package utils

import (
	"errors"
	"os"

	log "github.com/Sirupsen/logrus"
)

type Logger struct {
	AccessLog *log.Logger
	ErrorLog  *log.Logger
}

type logFileWriter struct {
	file *os.File
}

func (p *logFileWriter) Write(data []byte) (n int, err error) {
	if p == nil {
		return 0, errors.New("logFileWriter is nil")
	}
	if p.file == nil {
		return 0, errors.New("file not opened")
	}
	n, e := p.file.Write(data)
	return n, e
}

func LogNew(access_file string, error_file string) (logger *Logger) {

	af, err := os.OpenFile(access_file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic("打开访问日志文件失败")
	}
	ao := &logFileWriter{
		file: af,
	}
	logger = &Logger{}
	logger.AccessLog = log.New()
	logger.AccessLog.SetOutput(ao)

	ef, err1 := os.OpenFile(error_file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err1 != nil {
		panic("打开访问日志文件失败")
	}
	eo := &logFileWriter{
		file: ef,
	}
	logger.ErrorLog = log.New()
	logger.ErrorLog.SetOutput(eo)
	return logger
}

func (logger *Logger) Println(args ...interface{}) {
	// logger.AccessLog.WithFields(log.Fields{
	// 	"animal": "walrus",
	// }).Info(args...)
	logger.AccessLog.Info(args...)
}

func (logger *Logger) Error(args ...interface{}) {
	logger.ErrorLog.Info(args...)
}
