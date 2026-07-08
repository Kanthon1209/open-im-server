package push

import (
	"context"
	"encoding/json"
	"math/rand"
	"strconv"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"

	"github.com/IBM/sarama"
	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush"
	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/kafka"
	"github.com/openimsdk/open-im-server/v3/pkg/common/webhook"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/rpccache"
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/msggateway"
	pbpush "github.com/openimsdk/protocol/push"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/utils/datautil"
	"github.com/openimsdk/tools/utils/jsonutil"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type ConsumerHandler struct {
	pushConsumerGroup *kafka.MConsumerGroup // 内嵌一个 sarama.ConsumerGroup
	// 这是 push 服务的 kafka 消费处理对象
	// 内 pushConsumerGroup 是一个 MConsumerGroup 对象
	// 用来讲 ConsumerHandler 和 sarama.ConsumerGroup.Consume() 方法进行绑定

	// 下面依赖一堆这个消费实例需要依赖的实例
	offlinePusher offlinepush.OfflinePusher

	// 这个 OnlinePusher 也是有意思, 这个 ConsumerHandler 属于是 push 服务中核心的 ConsumerHandler
	// 消费 ToPush Topic 并进行下游业务逻辑的时候
	onlinePusher OnlinePusher // 专门用来 获取 msggateway 连接并进行推送的

	// push 服务在 controller 层的 Database 接口
	pushDatabase           controller.PushDatabase

	// 
	onlineCache            *rpccache.OnlineCache
	groupLocalCache        *rpccache.GroupLocalCache
	conversationLocalCache *rpccache.ConversationLocalCache
	webhookClient          *webhook.Client
	config                 *Config
	userClient             *rpcli.UserClient
	groupClient            *rpcli.GroupClient
	msgClient              *rpcli.MsgClient
	conversationClient     *rpcli.ConversationClient
}

// 看看这个 Handler 都是什么逻辑
// 核心的业务逻辑都绑定在 这个 ConsumerHandler 上, 并非 sarama 的
// 1. 创建 MConsumerGroup: 内嵌了 sarama.ConsumerGroup, 方法提升得到 Consume 等方法
//   - 内嵌是为了在外层额外提供方法, 将 Handler 和 sarama.ConsumerGroup 绑定到一起
//   - 方便 sarama.Consume 的时候直接用这个组合体中已经组合的 handler, 调用 组合体的注册方法, 方便
func NewConsumerHandler(ctx context.Context,
	config *Config,
	database controller.PushDatabase,
	offlinePusher offlinepush.OfflinePusher,
	rdb redis.UniversalClient,
	client discovery.SvcDiscoveryRegistry) (*ConsumerHandler, error) {
	var consumerHandler ConsumerHandler
	var err error
	// 为什么把 sarama.ConsumerGroup 包装在 ConsumerHandler 中?
	// 为的是 将 sarama.ConsumerGroupHandler 和 ConsumerGroup 绑定到一起
	// 这个 MConsumerGroup 内嵌的 sarama.ConsumerGroup 才是

	// ConsumerHandler 的 MGroupHandler 内嵌一个 ToPush Topic 的 sarama.ConsumerGroup
	consumerHandler.pushConsumerGroup, err = kafka.NewMConsumerGroup(config.KafkaConfig.Build(), config.KafkaConfig.ToPushGroupID,
		[]string{config.KafkaConfig.ToPushTopic}, true)
	if err != nil {
		return nil, err
	}
	// 通过 服务发现 client 拿到各个依赖的 grpc 服务的 连接: grpc.ClientConnInterface
	// 实现是 grpc.ClientConn, 底层是 HTTP 2.0 , 建立在 net.Conn 接口的实现: net.TCPConn
	// epoll: event poll, poll: 轮询
	// net.FD -> 非阻塞 IO, netpoller, epoll, 红黑树, 队列, epoll_wait, epitem
	// 在此基础上建立 HTTP 2.0 连接, 和目标 node 中的 pod 中的 container 中的 进程
	// 和 各个 rpc 服务建立连接
	// 	* rpc-user
	//	* rpc-group
	// 	* rpc-msg
	//	* rpc-conversation

	userConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.User)
	if err != nil {
		return nil, err
	}
	groupConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Group)
	if err != nil {
		return nil, err
	}
	msgConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Msg)
	if err != nil {
		return nil, err
	}
	conversationConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Conversation)
	if err != nil {
		return nil, err
	}

	//
	consumerHandler.userClient = rpcli.NewUserClient(userConn)
	consumerHandler.groupClient = rpcli.NewGroupClient(groupConn)
	consumerHandler.msgClient = rpcli.NewMsgClient(msgConn)
	consumerHandler.conversationClient = rpcli.NewConversationClient(conversationConn)

	consumerHandler.offlinePusher = offlinePusher
	// 创建一个 OnlinePusher, 给 pushUserIDs 发送到这些 IDs
	consumerHandler.onlinePusher = NewOnlinePusher(client, config)
	consumerHandler.groupLocalCache = rpccache.NewGroupLocalCache(consumerHandler.groupClient, &config.LocalCacheConfig, rdb)
	consumerHandler.conversationLocalCache = rpccache.NewConversationLocalCache(consumerHandler.conversationClient, &config.LocalCacheConfig, rdb)
	consumerHandler.webhookClient = webhook.NewWebhookClient(config.WebhooksConfig.URL)
	consumerHandler.config = config
	consumerHandler.pushDatabase = database
	consumerHandler.onlineCache, err = rpccache.NewOnlineCache(consumerHandler.userClient, consumerHandler.groupLocalCache, rdb, config.RpcConfig.FullUserCache, nil)
	if err != nil {
		return nil, err
	}
	return &consumerHandler, nil
}

func (c *ConsumerHandler) handleMs2PsChat(ctx context.Context, msg []byte) {
	// 将从 MQ 中拿到的 []byte unmarshal 到 pbpush.PushMsgReq 中
	msgFromMQ := pbpush.PushMsgReq{}
	// 将 从 MQ 拿到的 []byte unmashal 到 结构体
	if err := proto.Unmarshal(msg, &msgFromMQ); err != nil {
		log.ZError(ctx, "push Unmarshal msg err", err, "msg", string(msg))
		return
	}

	// 从 msg 拿出来 SendTime 秒数
	sec := msgFromMQ.MsgData.SendTime / 1000
	nowSec := time.Now().Unix() // 这样不就好了
	// nowSec := timeutil.GetCurrentTimestampBySecond()

	// 判断一下
	if nowSec-sec > 10 {
		prommetrics.MsgLoneTimePushCounter.Inc() // 统计过长时间到达 handle 函数的数量
		log.ZWarn(ctx, "it’s been a while since the message was sent", nil, "msg", msgFromMQ.String(), "sec", sec, "nowSec", nowSec, "nowSec-sec", nowSec-sec)
	}
	var err error

	switch msgFromMQ.MsgData.SessionType {
	// 从 MsgData.SessionType 中判断一下
	case constant.ReadGroupChatType:
		err = c.Push2Group(ctx, msgFromMQ.MsgData.GroupID, msgFromMQ.MsgData)
	default:
		var pushUserIDList []string
		isSenderSync := datautil.GetSwitchFromOptions(msgFromMQ.MsgData.Options, constant.IsSenderSync)
		// 判断一下要不要同步发给自己
		if !isSenderSync || msgFromMQ.MsgData.SendID == msgFromMQ.MsgData.RecvID {
			pushUserIDList = append(pushUserIDList, msgFromMQ.MsgData.RecvID)
		} else { // 就是要进行 sender sync, 那么就往 pushUserIDList 放上发送者的 ID
			pushUserIDList = append(pushUserIDList, msgFromMQ.MsgData.RecvID, msgFromMQ.MsgData.SendID)
		}
		// pushUserIDList 中存放所有推送的
		err = c.Push2User(ctx, pushUserIDList, msgFromMQ.MsgData)
	}
	if err != nil {
		log.ZWarn(ctx, "push failed", err, "msg", msgFromMQ.String())
	}
}

func (*ConsumerHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (*ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// 实现 ConsumerGroupHandler
// sarama 对每一个分区启动一个 goroutine, 每一个调用 ConsumeClaim
// 实现了 GroupConsumerHandler 接口
func (c *ConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 通过 claimMessages() 可以获得 一个 channel, 读循环获取的 msg 会不断塞入这个 channel
	// Consume 方法跑的是一次消费组会话
	// 这个 channel 就是 sarama.ConsumerGroupClaim.Messages()
	// ConsumeClaim 是会话建好、分区分下来之后，Sarama 为每个分到的分区起的读循环
	// 都循环读取的内容不断放到 sarama.ConsumerGroupClaim.Messages() 这个 channel 中
	c.onlineCache.Lock.Lock()
	for c.onlineCache.CurrentPhase.Load() < rpccache.DoSubscribeOver {
		c.onlineCache.Cond.Wait()
	}
	// ConsumeClaim：负责这一个分区。消息通道由 Sarama 填充，读和处理要写在这个方法里。
	c.onlineCache.Lock.Unlock()
	ctx := mcontext.SetOperationID(context.TODO(), strconv.FormatInt(time.Now().UnixNano()+int64(rand.Uint32()), 10))
	log.ZInfo(ctx, "begin consume messages")

	// claim.Messages
	for msg := range claim.Messages() {
		ctx := c.pushConsumerGroup.GetContextFromMsg(msg)
		ctx = mcontext.WithOpUserIDContext(ctx, c.config.Share.IMAdminUserID[0])
		// ConsumerHandler 上绑定 handlerMsg2PsChat 处理这个 msg
		// msg.Value 是 msg 对应的二进制内容,
		c.handleMs2PsChat(ctx, msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}

// Push2User Suitable for two types of conversations, one is SingleChatType and the other is NotificationChatType.
func (c *ConsumerHandler) Push2User(ctx context.Context, userIDs []string, msg *sdkws.MsgData) (err error) {
	log.ZInfo(ctx, "Get msg from msg_transfer And push msg", "userIDs", userIDs, "msg", msg.String())
	defer func(duration time.Time) {
		t := time.Since(duration)
		log.ZInfo(ctx, "Get msg from msg_transfer And push msg end", "msg", msg.String(), "time cost", t)
	}(time.Now())
	// 定义一个函数, 用来计算耗时多久
	// 在 OnlinePush 前调用 webhook
	if err := c.webhookBeforeOnlinePush(ctx, &c.config.WebhooksConfig.BeforeOnlinePush, userIDs, msg); err != nil {
		return err
	}

	//
	wsResults, err := c.GetConnsAndOnlinePush(ctx, msg, userIDs)
	if err != nil {
		return err
	}

	log.ZDebug(ctx, "single and notification push result", "result", wsResults, "msg", msg, "push_to_userID", userIDs)
	log.ZInfo(ctx, "single and notification push end")

	if !c.shouldPushOffline(ctx, msg) {
		return nil
	}
	log.ZInfo(ctx, "pushOffline start")

	for _, v := range wsResults {
		//message sender do not need offline push
		if msg.SendID == v.UserID {
			continue
		}
		//receiver online push success
		if v.OnlinePush {
			return nil
		}
	}
	needOfflinePushUserID := []string{msg.RecvID}
	var offlinePushUserID []string

	//receiver offline push
	if err = c.webhookBeforeOfflinePush(ctx, &c.config.WebhooksConfig.BeforeOfflinePush, needOfflinePushUserID, msg, &offlinePushUserID); err != nil {
		return err
	}

	if len(offlinePushUserID) > 0 {
		needOfflinePushUserID = offlinePushUserID
	}
	err = c.offlinePushMsg(ctx, msg, needOfflinePushUserID)
	if err != nil {
		log.ZDebug(ctx, "offlinePushMsg failed", err, "needOfflinePushUserID", needOfflinePushUserID, "msg", msg)
		log.ZWarn(ctx, "offlinePushMsg failed", err, "needOfflinePushUserID length", len(needOfflinePushUserID), "msg", msg)
		return nil
	}

	return nil
}

func (c *ConsumerHandler) shouldPushOffline(_ context.Context, msg *sdkws.MsgData) bool {
	isOfflinePush := datautil.GetSwitchFromOptions(msg.Options, constant.IsOfflinePush)
	if !isOfflinePush {
		return false
	}
	switch msg.ContentType {
	case constant.RoomParticipantsConnectedNotification:
		return false
	case constant.RoomParticipantsDisconnectedNotification:
		return false
	}
	return true
}

func (c *ConsumerHandler) GetConnsAndOnlinePush(ctx context.Context, msg *sdkws.MsgData, pushToUserIDs []string) ([]*msggateway.SingleMsgToUserResults, error) {
	if msg != nil && msg.Status == constant.MsgStatusSending {
		msg.Status = constant.MsgStatusSendSuccess
	}
	// 从 onlineCache 中看我们发送目标的 slice 中哪些在线, 哪些离线
	onlineUserIDs, offlineUserIDs, err := c.onlineCache.GetUsersOnline(ctx, pushToUserIDs)
	if err != nil {
		return nil, err
	}

	log.ZDebug(ctx, "GetConnsAndOnlinePush online cache", "sendID", msg.SendID, "recvID", msg.RecvID, "groupID", msg.GroupID, "sessionType", msg.SessionType, "clientMsgID", msg.ClientMsgID, "serverMsgID", msg.ServerMsgID, "offlineUserIDs", offlineUserIDs, "onlineUserIDs", onlineUserIDs)
	var result []*msggateway.SingleMsgToUserResults
	// 如果 有目标用户在线
	if len(onlineUserIDs) > 0 {
		var err error
		// onlinePusher 会去获取目标所在的 msggateway 的 hubserver 的 连接
		//
		result, err = c.onlinePusher.GetConnsAndOnlinePush(ctx, msg, onlineUserIDs)
		if err != nil {
			return nil, err
		}
	}
	for _, userID := range offlineUserIDs {
		result = append(result, &msggateway.SingleMsgToUserResults{
			UserID: userID,
		})
	}
	return result, nil
}

func (c *ConsumerHandler) Push2Group(ctx context.Context, groupID string, msg *sdkws.MsgData) (err error) {
	log.ZInfo(ctx, "Get group msg from msg_transfer and push msg", "msg", msg.String(), "groupID", groupID)
	defer func(duration time.Time) {
		t := time.Since(duration)
		log.ZInfo(ctx, "Get group msg from msg_transfer and push msg end", "msg", msg.String(), "groupID", groupID, "time cost", t)
	}(time.Now())
	var pushToUserIDs []string
	if err = c.webhookBeforeGroupOnlinePush(ctx, &c.config.WebhooksConfig.BeforeGroupOnlinePush, groupID, msg,
		&pushToUserIDs); err != nil {
		return err
	}

	err = c.groupMessagesHandler(ctx, groupID, &pushToUserIDs, msg)
	if err != nil {
		return err
	}

	wsResults, err := c.GetConnsAndOnlinePush(ctx, msg, pushToUserIDs)
	if err != nil {
		return err
	}

	log.ZDebug(ctx, "group push result", "result", wsResults, "msg", msg)
	log.ZInfo(ctx, "online group push end")

	if !c.shouldPushOffline(ctx, msg) {
		return nil
	}
	needOfflinePushUserIDs := c.onlinePusher.GetOnlinePushFailedUserIDs(ctx, msg, wsResults, &pushToUserIDs)
	//filter some user, like don not disturb or don't need offline push etc.
	needOfflinePushUserIDs, err = c.filterGroupMessageOfflinePush(ctx, groupID, msg, needOfflinePushUserIDs)
	if err != nil {
		return err
	}
	log.ZInfo(ctx, "filterGroupMessageOfflinePush end")

	// Use offline push messaging
	if len(needOfflinePushUserIDs) > 0 {
		c.asyncOfflinePush(ctx, needOfflinePushUserIDs, msg)
	}

	return nil
}

func (c *ConsumerHandler) asyncOfflinePush(ctx context.Context, needOfflinePushUserIDs []string, msg *sdkws.MsgData) {
	var offlinePushUserIDs []string
	err := c.webhookBeforeOfflinePush(ctx, &c.config.WebhooksConfig.BeforeOfflinePush, needOfflinePushUserIDs, msg, &offlinePushUserIDs)
	if err != nil {
		log.ZWarn(ctx, "webhookBeforeOfflinePush failed", err, "msg", msg)
		return
	}

	if len(offlinePushUserIDs) > 0 {
		needOfflinePushUserIDs = offlinePushUserIDs
	}
	if err := c.pushDatabase.MsgToOfflinePushMQ(ctx, conversationutil.GenConversationUniqueKeyForSingle(msg.SendID, msg.RecvID), needOfflinePushUserIDs, msg); err != nil {
		log.ZDebug(ctx, "Msg To OfflinePush MQ error", err, "needOfflinePushUserIDs",
			needOfflinePushUserIDs, "msg", msg)
		log.ZWarn(ctx, "Msg To OfflinePush MQ error", err, "needOfflinePushUserIDs length",
			len(needOfflinePushUserIDs), "msg", msg)
		prommetrics.GroupChatMsgProcessFailedCounter.Inc()
		return
	}
}

func (c *ConsumerHandler) groupMessagesHandler(ctx context.Context, groupID string, pushToUserIDs *[]string, msg *sdkws.MsgData) (err error) {
	if len(*pushToUserIDs) == 0 {
		*pushToUserIDs, err = c.groupLocalCache.GetGroupMemberIDs(ctx, groupID)
		if err != nil {
			return err
		}
		switch msg.ContentType {
		case constant.MemberQuitNotification:
			var tips sdkws.MemberQuitTips
			if unmarshalNotificationElem(msg.Content, &tips) != nil {
				return err
			}
			if err = c.DeleteMemberAndSetConversationSeq(ctx, groupID, []string{tips.QuitUser.UserID}); err != nil {
				log.ZError(ctx, "MemberQuitNotification DeleteMemberAndSetConversationSeq", err, "groupID", groupID, "userID", tips.QuitUser.UserID)
			}
			*pushToUserIDs = append(*pushToUserIDs, tips.QuitUser.UserID)
		case constant.MemberKickedNotification:
			var tips sdkws.MemberKickedTips
			if unmarshalNotificationElem(msg.Content, &tips) != nil {
				return err
			}
			kickedUsers := datautil.Slice(tips.KickedUserList, func(e *sdkws.GroupMemberFullInfo) string { return e.UserID })
			if err = c.DeleteMemberAndSetConversationSeq(ctx, groupID, kickedUsers); err != nil {
				log.ZError(ctx, "MemberKickedNotification DeleteMemberAndSetConversationSeq", err, "groupID", groupID, "userIDs", kickedUsers)
			}

			*pushToUserIDs = append(*pushToUserIDs, kickedUsers...)
		case constant.GroupDismissedNotification:
			if msgprocessor.IsNotification(msgprocessor.GetConversationIDByMsg(msg)) {
				var tips sdkws.GroupDismissedTips
				if unmarshalNotificationElem(msg.Content, &tips) != nil {
					return err
				}
				log.ZDebug(ctx, "GroupDismissedNotificationInfo****", "groupID", groupID, "num", len(*pushToUserIDs), "list", pushToUserIDs)
				if len(c.config.Share.IMAdminUserID) > 0 {
					ctx = mcontext.WithOpUserIDContext(ctx, c.config.Share.IMAdminUserID[0])
				}
				defer func(groupID string) {
					if err := c.groupClient.DismissGroup(ctx, groupID, true); err != nil {
						log.ZError(ctx, "DismissGroup Notification clear members", err, "groupID", groupID)
					}
				}(groupID)
			}
		}
	}
	return err
}

func (c *ConsumerHandler) offlinePushMsg(ctx context.Context, msg *sdkws.MsgData, offlinePushUserIDs []string) error {
	title, content, opts, err := c.getOfflinePushInfos(msg)
	if err != nil {
		log.ZError(ctx, "getOfflinePushInfos failed", err, "msg", msg)
		return err
	}
	err = c.offlinePusher.Push(ctx, offlinePushUserIDs, title, content, opts)
	if err != nil {
		prommetrics.MsgOfflinePushFailedCounter.Inc()
		return err
	}
	return nil
}

func (c *ConsumerHandler) filterGroupMessageOfflinePush(ctx context.Context, groupID string, msg *sdkws.MsgData,
	offlinePushUserIDs []string) (userIDs []string, err error) {
	needOfflinePushUserIDs, err := c.conversationClient.GetConversationOfflinePushUserIDs(ctx, conversationutil.GenGroupConversationID(groupID), offlinePushUserIDs)
	if err != nil {
		return nil, err
	}
	return needOfflinePushUserIDs, nil
}

func (c *ConsumerHandler) getOfflinePushInfos(msg *sdkws.MsgData) (title, content string, opts *options.Opts, err error) {
	type AtTextElem struct {
		Text       string   `json:"text,omitempty"`
		AtUserList []string `json:"atUserList,omitempty"`
		IsAtSelf   bool     `json:"isAtSelf"`
	}

	opts = &options.Opts{Signal: &options.Signal{ClientMsgID: msg.ClientMsgID}}
	if msg.OfflinePushInfo != nil {
		opts.IOSBadgeCount = msg.OfflinePushInfo.IOSBadgeCount
		opts.IOSPushSound = msg.OfflinePushInfo.IOSPushSound
		opts.Ex = msg.OfflinePushInfo.Ex
	}

	if msg.OfflinePushInfo != nil {
		title = msg.OfflinePushInfo.Title
		content = msg.OfflinePushInfo.Desc
	}
	if title == "" {
		switch msg.ContentType {
		case constant.Text:
			fallthrough
		case constant.Picture:
			fallthrough
		case constant.Voice:
			fallthrough
		case constant.Video:
			fallthrough
		case constant.File:
			title = constant.ContentType2PushContent[int64(msg.ContentType)]
		case constant.AtText:
			ac := AtTextElem{}
			_ = jsonutil.JsonStringToStruct(string(msg.Content), &ac)
		case constant.SignalingNotification:
			title = constant.ContentType2PushContent[constant.SignalMsg]
		default:
			title = constant.ContentType2PushContent[constant.Common]
		}
	}
	if content == "" {
		content = title
	}
	return
}

func (c *ConsumerHandler) DeleteMemberAndSetConversationSeq(ctx context.Context, groupID string, userIDs []string) error {
	conversationID := msgprocessor.GetConversationIDBySessionType(constant.ReadGroupChatType, groupID)
	maxSeq, err := c.msgClient.GetConversationMaxSeq(ctx, conversationID)
	if err != nil {
		return err
	}
	return c.conversationClient.SetConversationMaxSeq(ctx, conversationID, userIDs, maxSeq)
}

func unmarshalNotificationElem(bytes []byte, t any) error {
	var notification sdkws.NotificationElem
	if err := json.Unmarshal(bytes, &notification); err != nil {
		return err
	}
	return json.Unmarshal([]byte(notification.Detail), t)
}
