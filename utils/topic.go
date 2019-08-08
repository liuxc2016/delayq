package utils

type TopicSetting struct {
	MaxTryes int
	Ttr      []int64 //再执行的秒数
}

func GetTopicSetting(topic string) *TopicSetting {
	topicSetting := &TopicSetting{}
	if topic == "push_url" {
		topicSetting.MaxTryes = 5
		topicSetting.Ttr = []int64{30, 60, 90, 120}
	} else {
		topicSetting.MaxTryes = 1
		topicSetting.Ttr = []int64{30}
	}
	return topicSetting
}
