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
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"
	"github.com/openimsdk/tools/discovery"

	"github.com/go-redis/redis"
	"google.golang.org/protobuf/proto"

	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"

	"github.com/IBM/sarama"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/kafka"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/tools/batcher"
	"github.com/openimsdk/protocol/constant"
	pbconv "github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/utils/stringutil"
)

const (
	size              = 500
	mainDataBuffer    = 500
	subChanBuffer     = 50
	worker            = 50
	interval          = 100 * time.Millisecond
	hasReadChanBuffer = 1000
)

type ContextMsg struct {
	message *sdkws.MsgData
	ctx     context.Context
}

// This structure is used for asynchronously writing the sender’s read sequence (seq) regarding a message into MongoDB.
// For example, if the sender sends a message with a seq of 10, then their own read seq for this conversation should be set to 10.
type userHasReadSeq struct {
	conversationID string
	userHasReadMap map[string]int64
}

// OnlineHistoryRedisConsumerHandler 是 msgtransfer 里消费 ToRedisTopic 的成员。
// 本进程的一次 Consume 在 ToRedisGroupID 下对应一个成员；分到多个分区时，每个分区一个 ConsumeClaim goroutine，仍属这一个成员。
// 它同时实现 sarama.ConsumerGroupHandler：Setup、ConsumeClaim、Cleanup。
type OnlineHistoryRedisConsumerHandler struct {
	// 只订阅 ToRedisTopic，且关闭自动提交。offset 要等这批消息写完 Redis 再由 OnComplete 提交。
	historyConsumerGroup *kafka.MConsumerGroup // 具体业务的 ConsumerHandler 会组合一个 MConsumerGroup

	// 按 Kafka message key（会话唯一键）分片。同一会话始终进同一个 worker，避免打乱顺序。
	// 我们在 消费逻辑 ConsumeClaim 中不断把 Fetch 到的消息放到
	redisMessageBatches *batcher.Batcher[sarama.ConsumerMessage]

	msgTransferDatabase controller.MsgTransferDatabase
	// 已读序号先推进这里，HandleUserHasReadSeqMessages 再异步写入 MongoDB。
	conversationUserHasReadChan chan *userHasReadSeq
	// 给 HandleUserHasReadSeqMessages 用。Close 时关 channel 并 Wait。
	wg sync.WaitGroup

	// 写缓存时，新会话要靠这两条 RPC 建会话、查群成员。
	groupClient        *rpcli.GroupClient
	conversationClient *rpcli.ConversationClient
}

// NewOnlineHistoryRedisConsumerHandler 只组装依赖，此时还没有入组。
// JoinGroup 发生在 init.go 里随后启动的 RegisterHandleAndConsumer：那里对 historyConsumerGroup 调用 Consume，并把本 handler 作为 ConsumerGroupHandler 传进去。
//
// 构造做四件事：
//  1. 按 ToRedisGroupID 建消费者，只订阅 ToRedisTopic。最后一个参数 false 表示关掉自动提交。
//  2. 从服务发现拿到 group、conversation 两条 RPC 连接。
//  3. 打开长度 hasReadChanBuffer 的已读序号 channel，并 wg.Add(1)，对应后面的 HandleUserHasReadSeqMessages。
//  4. 装批处理器：凑满 size 条或满 interval 就调用 do；WithSyncWait(true) 表示这一批处理完，Put 才继续。
func NewOnlineHistoryRedisConsumerHandler(
	ctx context.Context,
	client discovery.SvcDiscoveryRegistry,
	config *Config,
	database controller.MsgTransferDatabase,
) (*OnlineHistoryRedisConsumerHandler, error) {
	kafkaConf := config.KafkaConfig
	// 创建 MConsumerGroup, 注意 MConsumerGroup 内嵌了 ConsumerGroup
	// 主要是为了组合必要的 Topic, ...
	// 绑定 注册 ConsumerHandler 方法到 Consume 中
	// MConsumerGroup 设置了 底层的 ConsumerGroup 会对 哪一个 Topic 进行 JoinGroup 操作
	//
	historyConsumerGroup, err := kafka.NewMConsumerGroup(kafkaConf.Build(), kafkaConf.ToRedisGroupID, []string{kafkaConf.ToRedisTopic}, false)
	if err != nil {
		return nil, err
	}

	// ConsumerHandler 对象就是消费 Kafka 中消息的核心
	// 当然依赖的一堆组件都要初始化好
	// 进行依赖的 各个 rpc 服务的 ClientConnInterface 的获取, GetConn 中通过 服务发现 服务获取到 地址, grpc.Dial
	groupConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Group)
	if err != nil {
		return nil, err
	}
	conversationConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Conversation)
	if err != nil {
		return nil, err
	}

	// 核心
	var och OnlineHistoryRedisConsumerHandler
	och.msgTransferDatabase = database
	och.conversationUserHasReadChan = make(chan *userHasReadSeq, hasReadChanBuffer)
	// group 服务 和 conversation 服务的 grpc client 的创建, 通过 grpc.ClientConnInterface
	och.groupClient = rpcli.NewGroupClient(groupConn)
	och.conversationClient = rpcli.NewConversationClient(conversationConn)
	// 与 HandleUserHasReadSeqMessages 里的 wg.Done 成对。那个循环负责把 channel 里的已读序号落到 MongoDB。
	och.wg.Add(1) // TODO: 暂时不知道为什么这么设计

	// size：一批最多多少条。worker：分片数量。interval：不满一批时多久强制刷出。
	// mainDataBuffer / subChanBuffer：批处理器内部队列容量。两级别的 chanBuffer
	// 用 withOptions 这中构建函数方法来创建出一个 Batcher
	b := batcher.New[sarama.ConsumerMessage](
		batcher.WithSize(size),
		batcher.WithWorker(worker),
		batcher.WithInterval(interval),
		batcher.WithDataBuffer(mainDataBuffer),
		batcher.WithSyncWait(true), // 把 SyncWait 开关打开
		// schedular 调度的时候会
		batcher.WithBuffer(subChanBuffer),
		// 不能用 withShading, withKeyFunc 把两个函数绑定吗
	)
	// Kafka 的 key 是会话唯一键。用它取模到 worker 数，同一个会话的消息始终进同一个 worker。
	// Batcher.Sharding 是一个 string -> int 的 func
	// 用于通过 key 将消息映射到 OnlineHistoryRedisConsumerHandler.Batcher 的 workers
	// 中的具体一个
	b.Sharding = func(key string) int {
		hashCode := stringutil.GetHashCode(key)
		return int(hashCode) % och.redisMessageBatches.Worker() // 返回 Worker 数量
	}
	// Batcher.Key 也是一个 func, 用于将 sarama.ConsumerMessage 提取得到 key string
	b.Key = func(consumerMessage *sarama.ConsumerMessage) string {
		return string(consumerMessage.Key)
	}
	// 一批凑满或到时间后执行。这里只做反序列化和落缓存，不在 ConsumeClaim 里逐条处理。
	b.Do = och.do // Batcher 会绑定 Do 方法
	// 这种混乱的绑定方式真是

	och.redisMessageBatches = b // 将 ConsumerHandler 的 Batcher 设置为这个
	och.historyConsumerGroup = historyConsumerGroup

	// 至此, 完成了 Batcher 的设置 ConsumerHandler 依赖的各个组件也设置好了
	// 关于具体的 ConsumeClaim 方法, 这个在 ConsumerHandler 中实现好了
	// Batcher.Do 方法, 这个 在 ConsumerHandler 中实现好了,
	// 初始化 ConsumerHandler 的时候绑定到持有的 Batcher 上
	return &och, nil
}

// do 处理一批同一个会话 key 的原始 Kafka 消息。
// 先 Unmarshal 成 sdkws.MsgData，处理已读回执，再按「要不要进历史、是不是通知」拆开。
// 要进历史的写入 Redis（BatchInsertChat2Cache）并更新已读序号；不进历史的直接转到 ToPush。
// 进了缓存的消息，随后由数据库层再转到 ToMongo、ToPush。

// Batcher.Do 方法其实就是这里定义的, Batcher 是一个通用的 类型, do 方法要结合具体业务来设计
// OnlineHistoryRedisConsumerHandler 是 一个具体的, ToRedis Topic 消费业务的 ConsumerHandler
// 会用到 MConsumerHandler, 会用到 Batcher, Batcher 的 具体处理函数要根据这个业务来实现
// 实现就放在了 Batcher 的外层, 初始化外层的时候对内层的 Batcher 进行 Do 函数的设置
func (och *OnlineHistoryRedisConsumerHandler) do(ctx context.Context, channelID int, val *batcher.Msg[sarama.ConsumerMessage]) {
	// Batcher的Do方法绑定的就是 ConsumerHandler.do 方法
	// 这里就是 kafka ToRedis 消息的真实处理位置
	// 数据流: kafka partition leader replica broker -- Fetch --> ConsumerGroup.Consume() 中的 每一个 Claim 的 ConsumeClaim goroutine
	// 在 ConsumerGroup.Consume 的 参数 ConsumerGroupHandler.ConsumeClaim 中实现消费逻辑
	// ConsumeClaim 中 从 claim.Messages() channel 中取 sarama.ConsumerMessage 中的 Value() []byte
	// 放到 ConsumerHandler 的 Batcher 中, 通过 Batcher.Put 方法
	// Batcher 的 Do 方法 绑定的 ConsumerHandler.do 方法, 在其中对 batcher.Msg 进行实际业务逻辑处理
	ctx = mcontext.WithTriggerIDContext(ctx, val.TriggerID())
	ctxMessages := och.parseConsumerMessages(ctx, val.Val())
	ctx = withAggregationCtx(ctx, ctxMessages)
	log.ZInfo(ctx, "msg arrived channel", "channel id", channelID, "msgList length", len(ctxMessages), "key", val.Key())
	och.doSetReadSeq(ctx, ctxMessages)

	// CH 上的 分类方法将 ctxMsgs 分到 不同的 slice 中
	storageMsgList, notStorageMsgList, storageNotificationList, notStorageNotificationList :=
		och.categorizeMessageLists(ctxMessages)
	log.ZDebug(ctx, "number of categorized messages", "storageMsgList", len(storageMsgList), "notStorageMsgList",
		len(notStorageMsgList), "storageNotificationList", len(storageNotificationList), "notStorageNotificationList", len(notStorageNotificationList))

	conversationIDMsg := msgprocessor.GetChatConversationIDByMsg(ctxMessages[0].message)
	conversationIDNotification := msgprocessor.GetNotificationConversationIDByMsg(ctxMessages[0].message)
	och.handleMsg(ctx, val.Key(), conversationIDMsg, storageMsgList, notStorageMsgList)
	och.handleNotification(ctx, val.Key(), conversationIDNotification, storageNotificationList, notStorageNotificationList)
}

func (och *OnlineHistoryRedisConsumerHandler) doSetReadSeq(ctx context.Context, msgs []*ContextMsg) {

	// Outer map: conversationID -> (userID -> maxHasReadSeq)
	conversationUserSeq := make(map[string]map[string]int64)

	for _, msg := range msgs {
		if msg.message.ContentType != constant.HasReadReceipt {
			continue
		}
		var elem sdkws.NotificationElem
		if err := json.Unmarshal(msg.message.Content, &elem); err != nil {
			log.ZWarn(ctx, "Unmarshal NotificationElem error", err, "msg", msg)
			continue
		}
		var tips sdkws.MarkAsReadTips
		if err := json.Unmarshal([]byte(elem.Detail), &tips); err != nil {
			log.ZWarn(ctx, "Unmarshal MarkAsReadTips error", err, "msg", msg)
			continue
		}
		if len(tips.ConversationID) == 0 || tips.HasReadSeq < 0 {
			continue
		}

		// Calculate the max seq from tips.Seqs
		for _, seq := range tips.Seqs {
			if tips.HasReadSeq < seq {
				tips.HasReadSeq = seq
			}
		}

		if _, ok := conversationUserSeq[tips.ConversationID]; !ok {
			conversationUserSeq[tips.ConversationID] = make(map[string]int64)
		}
		if conversationUserSeq[tips.ConversationID][tips.MarkAsReadUserID] < tips.HasReadSeq {
			conversationUserSeq[tips.ConversationID][tips.MarkAsReadUserID] = tips.HasReadSeq
		}
	}
	log.ZInfo(ctx, "doSetReadSeq", "conversationUserSeq", conversationUserSeq)

	// persist to db
	for convID, userSeqMap := range conversationUserSeq {
		if err := och.msgTransferDatabase.SetHasReadSeqToDB(ctx, convID, userSeqMap); err != nil {
			log.ZWarn(ctx, "SetHasReadSeqToDB error", err, "conversationID", convID, "userSeqMap", userSeqMap)
		}
	}

}

func (och *OnlineHistoryRedisConsumerHandler) parseConsumerMessages(ctx context.Context, consumerMessages []*sarama.ConsumerMessage) []*ContextMsg {
	var ctxMessages []*ContextMsg
	for i := 0; i < len(consumerMessages); i++ {
		ctxMsg := &ContextMsg{}
		msgFromMQ := &sdkws.MsgData{}
		err := proto.Unmarshal(consumerMessages[i].Value, msgFromMQ)
		if err != nil {
			log.ZWarn(ctx, "msg_transfer Unmarshal msg err", err, string(consumerMessages[i].Value))
			continue
		}
		var arr []string
		for i, header := range consumerMessages[i].Headers {
			arr = append(arr, strconv.Itoa(i), string(header.Key), string(header.Value))
		}
		log.ZDebug(ctx, "consumer.kafka.GetContextWithMQHeader", "len", len(consumerMessages[i].Headers),
			"header", strings.Join(arr, ", "))
		ctxMsg.ctx = kafka.GetContextWithMQHeader(consumerMessages[i].Headers)
		ctxMsg.message = msgFromMQ
		log.ZDebug(ctx, "message parse finish", "message", msgFromMQ, "key",
			string(consumerMessages[i].Key))
		ctxMessages = append(ctxMessages, ctxMsg)
	}
	return ctxMessages
}

// Get messages/notifications stored message list, not stored and pushed message list.
func (och *OnlineHistoryRedisConsumerHandler) categorizeMessageLists(totalMsgs []*ContextMsg) (storageMsgList,
	notStorageMsgList, storageNotificationList, notStorageNotificationList []*ContextMsg) {
	for _, v := range totalMsgs {
		options := msgprocessor.Options(v.message.Options)
		if !options.IsNotNotification() {
			// clone msg from notificationMsg
			if options.IsSendMsg() {
				msg := proto.Clone(v.message).(*sdkws.MsgData)
				// message
				if v.message.Options != nil {
					msg.Options = msgprocessor.NewMsgOptions()
				}
				msg.Options = msgprocessor.WithOptions(msg.Options,
					msgprocessor.WithOfflinePush(options.IsOfflinePush()),
					msgprocessor.WithUnreadCount(options.IsUnreadCount()),
				)
				v.message.Options = msgprocessor.WithOptions(
					v.message.Options,
					msgprocessor.WithOfflinePush(false),
					msgprocessor.WithUnreadCount(false),
				)
				ctxMsg := &ContextMsg{
					message: msg,
					ctx:     v.ctx,
				}
				storageMsgList = append(storageMsgList, ctxMsg)
			}
			if options.IsHistory() {
				storageNotificationList = append(storageNotificationList, v)
			} else {
				notStorageNotificationList = append(notStorageNotificationList, v)
			}
		} else {
			if options.IsHistory() {
				storageMsgList = append(storageMsgList, v)
			} else {
				notStorageMsgList = append(notStorageMsgList, v)
			}
		}
	}
	return
}

func (och *OnlineHistoryRedisConsumerHandler) handleMsg(ctx context.Context, key, conversationID string, storageList, notStorageList []*ContextMsg) {
	log.ZInfo(ctx, "handle storage msg")
	for _, storageMsg := range storageList {
		log.ZDebug(ctx, "handle storage msg", "msg", storageMsg.message.String())
	}

	och.toPushTopic(ctx, key, conversationID, notStorageList)
	var storageMessageList []*sdkws.MsgData
	for _, msg := range storageList {
		storageMessageList = append(storageMessageList, msg.message)
	}
	if len(storageMessageList) > 0 {
		msg := storageMessageList[0]
		lastSeq, isNewConversation, userSeqMap, err := och.msgTransferDatabase.BatchInsertChat2Cache(ctx, conversationID, storageMessageList)
		if err != nil && !errors.Is(errs.Unwrap(err), redis.Nil) {
			log.ZWarn(ctx, "batch data insert to redis err", err, "storageMsgList", storageMessageList)
			return
		}
		log.ZInfo(ctx, "BatchInsertChat2Cache end")
		err = och.msgTransferDatabase.SetHasReadSeqs(ctx, conversationID, userSeqMap)
		if err != nil {
			log.ZWarn(ctx, "SetHasReadSeqs error", err, "userSeqMap", userSeqMap, "conversationID", conversationID)
			prommetrics.SeqSetFailedCounter.Inc()
		}
		och.conversationUserHasReadChan <- &userHasReadSeq{
			conversationID: conversationID,
			userHasReadMap: userSeqMap,
		}

		if isNewConversation {
			ctx := storageList[0].ctx
			switch msg.SessionType {
			case constant.ReadGroupChatType:
				log.ZDebug(ctx, "group chat first create conversation", "conversationID",
					conversationID)

				userIDs, err := och.groupClient.GetGroupMemberUserIDs(ctx, msg.GroupID)
				if err != nil {
					log.ZWarn(ctx, "get group member ids error", err, "conversationID",
						conversationID)
				} else {
					log.ZInfo(ctx, "GetGroupMemberIDs end")

					if err := och.conversationClient.CreateGroupChatConversations(ctx, msg.GroupID, userIDs); err != nil {
						log.ZWarn(ctx, "single chat first create conversation error", err,
							"conversationID", conversationID)
					}
				}
			case constant.SingleChatType, constant.NotificationChatType:
				req := &pbconv.CreateSingleChatConversationsReq{
					RecvID:           msg.RecvID,
					SendID:           msg.SendID,
					ConversationID:   conversationID,
					ConversationType: msg.SessionType,
				}
				if err := och.conversationClient.CreateSingleChatConversations(ctx, req); err != nil {
					log.ZWarn(ctx, "single chat or notification first create conversation error", err,
						"conversationID", conversationID, "sessionType", msg.SessionType)
				}
			default:
				log.ZWarn(ctx, "unknown session type", nil, "sessionType",
					msg.SessionType)
			}
		}

		log.ZInfo(ctx, "success incr to next topic")
		err = och.msgTransferDatabase.MsgToMongoMQ(ctx, key, conversationID, storageMessageList, lastSeq)
		if err != nil {
			log.ZError(ctx, "Msg To MongoDB MQ error", err, "conversationID",
				conversationID, "storageList", storageMessageList, "lastSeq", lastSeq)
		}
		log.ZInfo(ctx, "MsgToMongoMQ end")

		och.toPushTopic(ctx, key, conversationID, storageList)
		log.ZInfo(ctx, "toPushTopic end")
	}
}

func (och *OnlineHistoryRedisConsumerHandler) handleNotification(ctx context.Context, key, conversationID string,
	storageList, notStorageList []*ContextMsg) {
	och.toPushTopic(ctx, key, conversationID, notStorageList)
	var storageMessageList []*sdkws.MsgData
	for _, msg := range storageList {
		storageMessageList = append(storageMessageList, msg.message)
	}
	if len(storageMessageList) > 0 {
		lastSeq, _, _, err := och.msgTransferDatabase.BatchInsertChat2Cache(ctx, conversationID, storageMessageList)
		if err != nil {
			log.ZError(ctx, "notification batch insert to redis error", err, "conversationID", conversationID,
				"storageList", storageMessageList)
			return
		}
		log.ZDebug(ctx, "success to next topic", "conversationID", conversationID)
		err = och.msgTransferDatabase.MsgToMongoMQ(ctx, key, conversationID, storageMessageList, lastSeq)
		if err != nil {
			log.ZError(ctx, "Msg To MongoDB MQ error", err, "conversationID",
				conversationID, "storageList", storageMessageList, "lastSeq", lastSeq)
		}
		och.toPushTopic(ctx, key, conversationID, storageList)
	}
}
func (och *OnlineHistoryRedisConsumerHandler) HandleUserHasReadSeqMessages(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.ZPanic(ctx, "HandleUserHasReadSeqMessages Panic", errs.ErrPanic(r))
		}
	}()

	defer och.wg.Done()

	for msg := range och.conversationUserHasReadChan {
		if err := och.msgTransferDatabase.SetHasReadSeqToDB(ctx, msg.conversationID, msg.userHasReadMap); err != nil {
			log.ZWarn(ctx, "set read seq to db error", err, "conversationID", msg.conversationID, "userSeqMap", msg.userHasReadMap)
		}
	}

	log.ZInfo(ctx, "Channel closed, exiting handleUserHasReadSeqMessages")
}
func (och *OnlineHistoryRedisConsumerHandler) Close() {
	close(och.conversationUserHasReadChan)
	och.wg.Wait()
}

func (och *OnlineHistoryRedisConsumerHandler) toPushTopic(ctx context.Context, key, conversationID string, msgs []*ContextMsg) {
	for _, v := range msgs {
		log.ZDebug(ctx, "push msg to topic", "msg", v.message.String())
		_, _, _ = och.msgTransferDatabase.MsgToPushMQ(v.ctx, key, conversationID, v.message)
	}
}

func withAggregationCtx(ctx context.Context, values []*ContextMsg) context.Context {
	var allMessageOperationID string
	for i, v := range values {
		if opid := mcontext.GetOperationID(v.ctx); opid != "" {
			if i == 0 {
				allMessageOperationID += opid
			} else {
				allMessageOperationID += "$" + opid
			}
		}
	}
	return mcontext.SetOperationID(ctx, allMessageOperationID)
}

// Setup、Cleanup 在每一代分配开始和结束时被 Sarama 调用。这一代的准备和收尾都不在这里做。
func (och *OnlineHistoryRedisConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }
func (och *OnlineHistoryRedisConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 是这个成员对一个分区的消费循环，由 Sarama 在 SyncGroup 之后按分区起 goroutine 调用。
// claim 是这一代里、本成员对这一个分区的消费权。本方法不解析消息，只把 claim.Messages() 里的原始记录丢进批处理器。
// Messages() 由 Sarama 向该分区的 leader broker 循环 Fetch 后填入；本方法从 channel 取出即可。
// 这一代结束时 session.Context() 被取消，或 Messages() 被关闭，循环返回，Consume 随之结束，外层会重新 JoinGroup。
//
// 自动提交是关着的。一批处理完，OnComplete 才对该批最后一条 MarkMessage 并 Commit，向 coordinator 发一次 OffsetCommit。
// MarkMessage 只在本进程记下位置，Commit 才是发给 coordinator 的请求，不走心跳，也不是每条消息一次。
func (och *OnlineHistoryRedisConsumerHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	log.ZDebug(context.Background(), "online new session msg come", "highWaterMarkOffset",
		claim.HighWaterMarkOffset(), "topic", claim.Topic(), "partition", claim.Partition())
	// och: online consumer handler
	// 注册回调函数给
	och.redisMessageBatches.OnComplete = func(lastMessage *sarama.ConsumerMessage, totalCount int) {
		session.MarkMessage(lastMessage, "")
		session.Commit()
	}
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil // 什么情况下会 不 ok, 取出来 channel 类型 零值?
				// 在 channel 被关闭后, 仍然可以从 channel 中 取 数据, 数据是类型零值
				// 但是不能向 被 close 后的 channel 中 发送数据, 会 panic
			}

			// msg.Value 是 []byte
			// 也就是说, 消费的这一条消息
			if len(msg.Value) == 0 {
				continue
			}

			// ** 获取到的 sarama.ConsumerMessage 塞入到 Batches 中
			// Batcher 那边会有 worker 循环拿数据放到 Do 中处理
			// 放到 Batcher 中的数据是 ConsumerMessage
			err := och.redisMessageBatches.Put(context.Background(), msg)
			if err != nil {
				log.ZWarn(context.Background(), "put msg to  error", err, "msg", msg)
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
