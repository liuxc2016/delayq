package delayq

const (
	READY_POOL_KEY_PREFIX string = "dqready:"
	JOBLIST_KEY_PREFIX    string = "dqjobs:"

	DELAY_BUCKET_KEY   string = "dqbucket"   //zset
	RESERVE_BUCKET_KEY string = "dqreserved" //zset
)

const (
	STATE_DELAY = iota
	STATE_READY
	STATE_RESERVE
	STATE_DELETE
)
