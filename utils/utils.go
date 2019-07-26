package utils

import (
	"strconv"
)

func String2int(str string) int {
	ret, err := strconv.Atoi(str)
	if err != nil {
		ret = 0
	}
	return ret
}

func String2int64(str string) int64 {
	ret, err := strconv.Atoi(str)
	if err != nil {
		ret = 0
	}
	return int64(ret)
}
