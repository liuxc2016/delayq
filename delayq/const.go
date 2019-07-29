package delayq

const (
	JOBLIST_KEY_PREFIX string = "dqjobs:"

	DELAY_BUCKET_KEY      string = "dqbucket"   //zset  --state=0
	READY_POOL_KEY_PREFIX string = "dqready:"   // --state=1
	RESERVE_BUCKET_KEY    string = "dqreserved" //list   --state=2

	FAIL_BUCKET_KEY   string = "dqfail"   //list    --state=3  达到最大次数后再算失败，或者删除掉认为是执行失败。
	FINISH_BUCKET_KEY string = "dqfinish" //list    --state=4  成功的任务id
)

const (
	STATE_DELAY = iota
	STATE_READY
	STATE_RESERVE
	STATE_DELETE
	STATE_FINISH
)
