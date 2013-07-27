package riak

const (
	rpbErrorResp = iota
	rpbPingReq
	rpbPingResp
	rpbGetClientIdReq
	rpbGetClientIdResp
	rpbSetClientIdReq
	rpbSetClientIdResp
	rpbGetServerInfoReq
	rpbGetServerInfoResp
	rpbGetReq
	rpbGetResp
	rpbPutReq
	rpbPutResp
	rpbDelReq
	rpbDelResp
	rpbListBucketsReq
	rpbListBucketsResp
	rpbListKeysReq
	rpbListKeysResp
	rpbGetBucketReq
	rpbGetBucketResp
	rpbSetBucketReq
	rpbSetBucketResp
	rpbMapRedReq
	rpbMapRedResp
	rpbIndexReq
	rpbIndexResp
	rpbSearchQueryReq
	rbpSearchQueryResp
	rpbCounterUpdateReq  = 50
	rpbCounterUpdateResp = 51
	rpbCounterGetReq     = 52
	rpbCounterGetResp    = 53
)
