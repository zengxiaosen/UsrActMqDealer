package main

import (
	"UsrActMqDealer/com/z/common"
	queue "UsrActMqDealer/com/z/utils/timeoutQueueUtils"
	config "configCollection"
	"context"
	"github.com/Shopify/sarama"
	"github.com/W1llyu/ourjson"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ConsumeActor struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
}

/**
依次类推（重复代码暂时省略了）:
feed五分钟内实时点击、曝光；
feed五分钟内实时平均播完率；
feed两个小时内实时点击、曝光；
feed两个小时内实时平均播完率；
feed四个小时内实时点击、曝光；
feed四个小时内实时平均播完率；
 */

func (consumeActor *ConsumeActor) consumerUsrClickShow5MinDealer() {
	tq := queue.TimeQueueWithTimeStep(common.TumblingDuration, 50, 1*time.Second)
	tq.StartTimeSpying()
	//用户每五秒内实时点击、曝光
	consumeActor.sliceUsrClickShow5Second(tq)
}

func (consumeActor *ConsumeActor) consumerUsrRpt5MinDealer() {
	//tumbling: slice = common.TumblingDuration
	//slice way: slice = 1*time.Second
	tq := queue.TimeQueueWithTimeStep(common.TumblingDuration, 50, 1*time.Second)
	tq.StartTimeSpying()
	//用户每五秒内实时平均播完率；
	consumeActor.sliceUsrRpt5Second(tq)
}

func (consumeActor *ConsumeActor)sliceUsrRpt5Second(tq *queue.Queue) {
	address := strings.Split(config.KAFKA_SERVER_4_VV, ",")
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		panic(err)
	}
	partitionList, err := consumer.Partitions(config.KAFKA_TOPIC_4_ACT)
	if err != nil {
		panic(err)
	}

	{
		var wg = &sync.WaitGroup{}
		for partition := range partitionList {
			//如果分区消费者已经消费了该信息，将会返回error
			//sarama.OffsetNewest:表明了为最新消息
			//ConsumerPartition方法根据主题，分区和给定的偏移量创建了响应的分区消费者
			pc, err := consumer.ConsumePartition(config.KAFKA_TOPIC_4_VV, int32(partition), sarama.OffsetNewest)
			if err != nil {
				panic(err)
			}
			defer pc.AsyncClose()
			wg.Add(1)
			go func() {
				defer wg.Done()
				for msg := range pc.Messages() {
					usrRptRecord := getUsrRptRecord(msg)
					if usrRptRecord == nil {
						continue
					}
					usrRptQData := common.VVRecord{}
					usrRptQData.U = usrRptRecord.U
					usrRptQData.Contid = usrRptRecord.Contid
					usrRptQData.Tm = usrRptRecord.Tm
					usrRptQData.Duration = usrRptRecord.Duration
					usrRptQData.Rpt = usrRptRecord.Rpt
					tq.SafeTPush(usrRptQData)
				}
			}()
		}
		wg.Wait()
	}
	consumer.Close()
}

func (consumeActor *ConsumeActor)sliceUsrClickShow5Second(tq *queue.Queue) {
	//init consumer
	address := strings.Split(config.KAFKA_SERVER_4_ACT, ",")
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		panic(err)
	}
	partitionList, err := consumer.Partitions(config.KAFKA_TOPIC_4_ACT)
	if err != nil {
		panic(err)
	}
	{
		var wg = &sync.WaitGroup{}
		for partition := range partitionList {
			//如果该分区消费者已经消费了该信息，将会返回error
			//sarama.OffsetNewest:表明了为最新消息
			//ConsumerPartition方法根据主题，分区和给定的偏移量创建了响应的分区消费者
			pc, err := consumer.ConsumePartition(config.KAFKA_TOPIC_4_ACT, int32(partition), sarama.OffsetNewest)
			if err != nil {
				panic(err)
			}
			defer pc.AsyncClose()
			wg.Add(1)
			go func() {
				defer wg.Done()
				//Message()方法返回一个消费消息类型的只读通道，由代理产生
				for msg := range pc.Messages() {
					usrClickShowRecord := getUsrClickShowRecord(msg)
					if usrClickShowRecord == nil {
						continue
					}
					//processingTime := time.Now().UnixNano() / 1e6
					usrClickShowQData := common.UsrClickShowRecord{}
					usrClickShowQData.U = usrClickShowRecord.U
					usrClickShowQData.ClickCount = usrClickShowRecord.ClickCount
					usrClickShowQData.ShowCount = usrClickShowRecord.ShowCount
					tq.SafeTPush(usrClickShowQData)
				}
			}()
		}
		wg.Wait()
	}
	consumer.Close()
}

func getUsrRptRecord(msg *sarama.ConsumerMessage) *common.VVRecord {
	jsonObject, err := ourjson.ParseObject(string(msg.Value))
	if err != nil {
		return nil
	}
	t, actErr := jsonObject.GetString(config.ACT_TYPE)
	u, uErr := jsonObject.GetString(config.U)
	contid, contidErr := jsonObject.GetString("contid")
	tm, tmErr := jsonObject.GetString(config.TM)
	duration, durationErr := jsonObject.GetString(config.DURATION)
	if t == "" || contid == "" || u == "" || tm == "" || duration == "" || actErr != nil || uErr != nil || contidErr != nil ||
		tmErr != nil || durationErr != nil {
		return nil
	}
	u = strings.Trim(u, "")
	t = strings.Trim(t, "")
	contid = strings.Trim(contid, "")
	tm = strings.Trim(tm, "")
	duration = strings.Trim(duration, "")
	tmFloat, errTm := strconv.ParseFloat(tm, 64)
	durationFloat, errDuration := strconv.ParseFloat(duration, 64)
	if errTm != nil || errDuration != nil {
		return nil
	}
	vvRecord := &common.VVRecord{u, contid, tmFloat, durationFloat,
		tmFloat / durationFloat}
	return vvRecord
}

func getUsrClickShowRecord(msg *sarama.ConsumerMessage) *common.UsrClickShowRecord {
	jsonObject, err := ourjson.ParseObject(string(msg.Value))
	if err != nil {
		return nil
	}
	t, actErr := jsonObject.GetString(config.ACT_TYPE)
	u, uErr := jsonObject.GetString(config.U)
	contentid, contidErr := jsonObject.GetString("contid")
	if actErr != nil || uErr != nil || contidErr != nil || t == "" ||
		contentid == "" || u == "" {
		return nil
	}
	u = strings.Trim(u, " ")
	t = strings.Trim(t, "")
	contentid = strings.Trim(contentid, "")
	usrActionRecord := &common.UsrActionRecord{u, t, contentid, 1}
	if usrActionRecord == nil {
		return nil
	}
	if usrActionRecord.T != config.CLICK_CONTENT && usrActionRecord.T != config.SHOW_CONTENT {
		return nil
	}
	usrClickShowRecord := &common.UsrClickShowRecord{}
	usrClickShowRecord.U = u
	if usrActionRecord.T == config.SHOW_CONTENT {
		usrClickShowRecord.ClickCount = 0
		usrClickShowRecord.ShowCount = usrActionRecord.Count
	} else {
		usrClickShowRecord.ClickCount = usrActionRecord.Count
		usrClickShowRecord.ShowCount = 0
	}
	return usrClickShowRecord
}
