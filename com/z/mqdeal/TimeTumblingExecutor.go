package main

import (
	"UsrActMqDealer/com/z/common"
	"UsrActMqDealer/com/z/storage"
	queueUtil "UsrActMqDealer/com/z/utils/timeoutQueueUtils"
	"container/list"
	"fmt"
	"reflect"
)

type TimeTumblingExecutor struct {
	//context.WithCancel(context.Background())
}

var (
	G_executor *TimeTumblingExecutor
)

func InitExecutor() {
	G_executor = &TimeTumblingExecutor{}
}
func (timeTumblingExecutor *TimeTumblingExecutor) getTumblingWrapper() {
	go func() {
		for {
			select {
			case wrappersChan := <-common.UsrAct5MinQueue:
				if wrappersChan != nil && len(wrappersChan.([]interface{})) > 1 {
					recordList := wrappersChan.([]interface{})
					actList := new(list.List)
					for _, record := range recordList {
						//判断类型
						queueRawRecord := record.(queueUtil.TimeWrapper).Data
						if reflect.TypeOf(queueRawRecord).Name() == "UsrClickShowRecord" {
							usrClickShowQRecord := queueRawRecord.(common.UsrClickShowRecord)
							actList.PushBack(usrClickShowQRecord)
						} else {
							continue
						}
					}
					batchReduceActRes := batchActReduceByU(actList)
					//sink
					if len(batchReduceActRes) > 0 {
						fmt.Println("batchReduceActRes:")
						fmt.Println(batchReduceActRes)
						storage.SinkActResToRedis(batchReduceActRes)
					}
				}
			case wrappersChan := <-common.UsrRpt5MinQueue:
				if wrappersChan != nil && len(wrappersChan.([]interface{})) > 1 {
					recordList := wrappersChan.([]interface{})
					vvList := new(list.List)
					for _, record := range recordList {
						//判断类型
						queueRawRecord := record.(queueUtil.TimeWrapper).Data
						if reflect.TypeOf(queueRawRecord).Name() == "VVRecord" {
							vvQRecord := queueRawRecord.(common.VVRecord)
							vvList.PushBack(vvQRecord)
						} else {
							continue
						}
					}
					batchReduceVVRes := batchVVReduceByU(vvList)
					//sink
					if len(batchReduceVVRes) > 0 {
						fmt.Println("batchReduceVVRes:")
						fmt.Println(batchReduceVVRes)
					}
				}
			}
		}
	}()
}

func batchVVReduceByU(vvList *list.List) map[string]common.VVSinkURecord {
	uVVSinkRecord := make(map[string]common.VVSinkURecord)
	for i := vvList.Front(); i != nil; i = i.Next() {
		record := i.Value.(common.VVRecord)
		if v, ok := uVVSinkRecord[record.U]; ok {
			v.Tm += record.Tm
			v.Duration += record.Duration
			v.Rpt = v.Tm / v.Duration
			uVVSinkRecord[record.U] = v
		} else {
			uRecord := common.VVSinkURecord{}
			uRecord.U = record.U
			uRecord.Tm = record.Tm
			uRecord.Duration = record.Duration
			uRecord.Rpt = record.Rpt
			uVVSinkRecord[record.U] = uRecord
		}
	}
	return uVVSinkRecord
}

func batchActReduceByU(batchRecordList *list.List) map[string]common.UsrClickShowRecord {
	uClickShow := make(map[string]common.UsrClickShowRecord)
	for i := batchRecordList.Front(); i != nil; i = i.Next() {
		record := i.Value.(common.UsrClickShowRecord)
		if v, ok := uClickShow[record.U]; ok {
			v.ClickCount += record.ClickCount
			v.ShowCount += record.ShowCount
			uClickShow[record.U] = v
		} else {
			uRecord := common.UsrClickShowRecord{}
			uRecord.U = record.U
			uRecord.ClickCount = record.ClickCount
			uRecord.ShowCount = record.ShowCount
			uClickShow[record.U] = uRecord
		}
	}
	return uClickShow
}
