// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msgtransfer

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/kafka"
	"github.com/openimsdk/open-im-server/v3/pkg/common/webhook"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/tools/log"
	"google.golang.org/protobuf/proto"
)

type OnlineHistoryMongoConsumerHandler struct {
	historyConsumerGroup *kafka.MConsumerGroup
	msgTransferDatabase  controller.MsgTransferDatabase
	config               *Config
	webhookClient        *webhook.Client
}

func NewOnlineHistoryMongoConsumerHandler(kafkaConf *config.Kafka, database controller.MsgTransferDatabase, config *Config) (*OnlineHistoryMongoConsumerHandler, error) {
	historyConsumerGroup, err := kafka.NewMConsumerGroup(kafkaConf.Build(), kafkaConf.ToMongoGroupID, []string{kafkaConf.ToMongoTopic}, true)
	if err != nil {
		return nil, err
	}

	mc := &OnlineHistoryMongoConsumerHandler{
		historyConsumerGroup: historyConsumerGroup,
		msgTransferDatabase:  database,
		config:               config,
		webhookClient:        webhook.NewWebhookClient(config.WebhooksConfig.URL),
	}
	return mc, nil
}

func (mc *OnlineHistoryMongoConsumerHandler) handleChatWs2Mongo(ctx context.Context, cMsg *sarama.ConsumerMessage, key string, session sarama.ConsumerGroupSession) {
	// 处理
	msg := cMsg.Value
	msgFromMQ := pbmsg.MsgDataToMongoByMQ{}
	err := proto.Unmarshal(msg, &msgFromMQ)
	if err != nil {
		log.ZError(ctx, "unmarshall failed", err, "key", key, "len", len(msg))
		return
	}
	if len(msgFromMQ.MsgData) == 0 {
		log.ZError(ctx, "msgFromMQ.MsgData is empty", nil, "cMsg", cMsg)
		return
	}
	log.ZDebug(ctx, "mongo consumer recv msg", "msgs", msgFromMQ.String())
	// 调用 controller 层的 msgTransferDatabase 的 BatchInsertChat2DB 将批量的 msgs 落库
	// 
	err = mc.msgTransferDatabase.BatchInsertChat2DB(ctx, msgFromMQ.ConversationID, msgFromMQ.MsgData, msgFromMQ.LastSeq)
	if err != nil {
		log.ZError(
			ctx,
			"single data insert to mongo err",
			err,
			"msg",
			msgFromMQ.MsgData,
			"conversationID",
			msgFromMQ.ConversationID,
		)
		prommetrics.MsgInsertMongoFailedCounter.Inc()
	} else {
		prommetrics.MsgInsertMongoSuccessCounter.Inc()
		for _, msgData := range msgFromMQ.MsgData {
			mc.webhookAfterMsgSaveDB(ctx, &mc.config.WebhooksConfig.AfterMsgSaveDB, msgData)
		}
	}
	//var seqs []int64
	//for _, msg := range msgFromMQ.MsgData {
	//	seqs = append(seqs, msg.Seq)
	//}
	//if err := mc.msgTransferDatabase.DeleteMessagesFromCache(ctx, msgFromMQ.ConversationID, seqs); err != nil {
	//	log.ZError(ctx, "remove cache msg from redis err", err, "msg",
	//		msgFromMQ.MsgData, "conversationID", msgFromMQ.ConversationID)
	//}
}

func (*OnlineHistoryMongoConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// 因为用不到这个函数, 用不到 OnlineHistoryMongoConsumerHandler, 所以没有给 接收者变量写名字
// 没有给函数参数写名字, 只写了一个占位符, 需要实现这个方法, 要不然实现不了接口
func (*OnlineHistoryMongoConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (mc *OnlineHistoryMongoConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error { // an instance in the consumer group
	// 实现 ConsumerGroupHandler 方法
	// 这是一个回调函数, Consume 方法中会调用, claim.Messages() 是 channel
	// 从 partition leader replica 所在 broker 源源不断 Fetch 过来的消息
	// 放到 sarama.ConsumerGroupClaim.Messages() 中
	log.ZDebug(context.Background(), "online new session msg come", "highWaterMarkOffset",
		claim.HighWaterMarkOffset(), "topic", claim.Topic(), "partition", claim.Partition())
	for msg := range claim.Messages() {
		ctx := mc.historyConsumerGroup.GetContextFromMsg(msg)
		if len(msg.Value) != 0 {
			// 消费 ToRedis Topic 消息, 到
			mc.handleChatWs2Mongo(ctx, msg, string(msg.Key), sess)
		} else {
			log.ZError(ctx, "mongo msg get from kafka but is nil", nil, "conversationID", msg.Key)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}
