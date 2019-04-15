package common

import "time"

var UsrAct5MinQueue chan interface{}
func InitUsrAct5MinQueueMsgChan() {
	UsrAct5MinQueue = make(chan interface{}, 100)
}

var UsrRpt5MinQueue chan interface{}
func InitUsrRpt5MinQueueMsgChan()  {
	UsrRpt5MinQueue = make(chan interface{})
}
const (
	TumblingDuration = 5*time.Second
	LOCK_PREFIX_OF_USERINFO = "LOCK_PREFIX_OF_USERINFO_%s"
	KEY_PREFIX_OF_USER_CLICK_SHOW_5MIN = "USER_CLICK_SHOW_5MIN_%s"
	LOCK_SUCCESS = "OK"
	SET_IF_NOT_EXIST = "NX"
	SET_WITH_EX = "EX"
	RELEASE_SUCCESS = int64(1)
	GET_LOCK_FAIL = 1
	UNLOCK_FAIL = 2
)

