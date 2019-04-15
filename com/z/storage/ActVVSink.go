package storage

import (
	"UsrActMqDealer/com/z/common"
	"UsrActMqDealer/com/z/pb"
	"UsrActMqDealer/com/z/storageclient/redisclient"
	"UsrActMqDealer/com/z/utils/redisUtils"
	"fmt"
	"github.com/satori/go.uuid"
)

func SinkActResToRedis(batchReduceActRes map[string]common.UsrClickShowRecord) {
	//conn, err := redis.Dial("tcp", "localhost:6379")
	conn := redisclient.Get()
	defer conn.Close()

	for u, uActInfo := range batchReduceActRes {
		lockKey := fmt.Sprintf(common.LOCK_PREFIX_OF_USERINFO, u)
		redisKey := fmt.Sprintf(common.KEY_PREFIX_OF_USER_CLICK_SHOW_5MIN, u)
		requestId := uuid.Must(uuid.NewV4())
		lock := redisUtils.Lock{}
		lock.Conn = conn
		lock.Resource = redisKey
		lock.Timeout = 2
		lock.Token = fmt.Sprintf("%d", requestId)
		m, err := lock.DoWithLock(lockKey, int(1), conn, redisUtils.SetUsrInfoOperation{}, redisKey, uActInfo)
		if err != nil {
			continue
		}
		//log
		fmt.Println("log for sink ")
		fmt.Println(m.(*sliceActVV.UserActInfo))
	}
}
