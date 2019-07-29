package utils

type TopicSetting struct {
	MaxTryes int
	Ttr      []int64 //再执行的秒数
}

func GetTopicSetting(topic string) *TopicSetting {
	topicSetting := &TopicSetting{}
	/*当前设置， ttr切片第0个元素是无效的。即job的第一次超时时间由addjob时给出*/
	if topic == "push_url" {
		topicSetting.MaxTryes = 3
		topicSetting.Ttr = []int64{10, 30, 120, 300}
	} else {
		topicSetting.MaxTryes = 1
		topicSetting.Ttr = []int64{30}
	}
	return topicSetting
}
