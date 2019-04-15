package redisUtils

import (
	"UsrActMqDealer/com/z/common"
	"UsrActMqDealer/com/z/pb"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	"log"
)

var (
	delScript = redis.NewScript(1, `
	if redis.call("get", KEYS[1]) == ARGV[1] then 
		return redis.call("del", KEYS[1]) 
	else 
		return 0
	end`)
)

type Lock struct {
	Resource string
	Token    string
	Conn     redis.Conn
	Timeout  int
}

func (lock *Lock) tryLock() error {
	//timeout单位为秒
	lockReply, err := lock.Conn.Do("SET", lock.Resource, lock.Token, common.SET_WITH_EX, lock.Timeout,
		common.SET_IF_NOT_EXIST)
	if err != nil {
		return errors.New("redis fail")
	}
	if lockReply == common.LOCK_SUCCESS {
		return nil
	} else {
		return errors.New("lock fail")
	}
}

func (lock *Lock) Unlock() error {
	_, err := delScript.Do(lock.Conn, lock.Resource, lock.Token)
	if err != nil {
		log.Fatal(fmt.Sprintf("unlock failed, err: %s", err))
	}
	return err
}

func (lock *Lock) key() string {
	res := lock.Resource
	return res
}

func (lock *Lock) AddTimeout(ex_time int64) (ok bool, err error) {
	ttl_time, err := redis.Int64(lock.Conn.Do("TTL", lock.Resource))
	if err != nil {
		log.Fatal()
	}

	if ttl_time > 0 {
		_, err := redis.String(lock.Conn.Do("SET", lock.Resource, lock.Token, common.SET_WITH_EX,
			int(ttl_time+ex_time)))
		if err == redis.ErrNil {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func TryLock(conn redis.Conn, resource string, token string, DefaultTimeout int) (lock *Lock, err error) {
	return TryLockWithTimeout(conn, resource, token, DefaultTimeout)
}

func TryLockWithTimeout(conn redis.Conn, resource string, token string, timeout int) (lock *Lock, err error) {
	lock = &Lock{resource, token, conn, timeout}
	err = lock.tryLock()
	if err != nil {
		lock = nil
		log.Fatal("try lock failed")
	}
	return
}

type RedisOperation interface {
	Execute(conn redis.Conn, redisKey string, v ...interface{}) (m interface{}, err error)
}

type SetUsrInfoOperation struct {

}

func (s SetUsrInfoOperation) Execute(conn redis.Conn, redisKey string, v ...interface{}) (m interface{}, err error) {
	record := v[0].([]interface{})[0]
	usrInfo := record.(common.UsrClickShowRecord)
	return SetUsrInfo(conn, redisKey, usrInfo)
}

func SetUsrInfo(conn redis.Conn, redisKey string, usrInfo common.UsrClickShowRecord)(m interface{}, err error) {
	fmt.Println("operation rediskey: " , redisKey)
	if getResult, err := conn.Do("get", redisKey); err != nil {
		return nil, err
	} else {
		if getResult == nil {
			usrMsg := &sliceActVV.UserActInfo{}
			usrMsg.RtUClick = &usrInfo.ClickCount
			usrMsg.RtUReveal= &usrInfo.ShowCount
			byts, err := proto.Marshal(usrMsg)
			if err != nil {
				return nil, err
			}
			if _, err := conn.Do("set", redisKey, byts); err != nil {
				return nil, err
			}
			return usrMsg, nil
		} else {
			usrInfoBytes := getResult.([]byte)
			usrMsg := &sliceActVV.UserActInfo{}
			err = proto.Unmarshal(usrInfoBytes, usrMsg)
			if err != nil {
				return nil, err
			}
			uClickCount := *usrMsg.RtUClick
			uClickCount += usrInfo.ClickCount
			usrMsg.RtUClick = &uClickCount

			uShowCount := *usrMsg.RtUReveal
			uShowCount += usrInfo.ShowCount
			usrMsg.RtUReveal = &uShowCount
			return usrMsg, nil
		}
	}
}

func (lock *Lock) DoWithLock(lockKey string, expire int, conn redis.Conn, operation RedisOperation, redisKey string,
	v ...interface{}) (m interface{}, err error) {
	lock, err = TryLock(conn, lockKey, lock.Token, expire)
	defer lock.Unlock()
	if err != nil {
		log.Fatal("Error while getting lock")
		m = nil
		return
	}
	res, err := operation.Execute(conn, redisKey, v)
	m = res
	return
}

