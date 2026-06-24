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

package msg

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/protocol/constant"
	pbconversation "github.com/openimsdk/protocol/conversation"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/protocol/wrapperspb"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/utils/datautil"
)

// SendMsg 会根据 MsgData 中的 SessionType 将 MsgData 分发给不同的函数继续往下走
func (m *msgServer) SendMsg(ctx context.Context, req *pbmsg.SendMsgReq) (*pbmsg.SendMsgResp, error) {
	if req.MsgData != nil {
		// 分发前补全服务端字段，并按 ContentType 调整发送开关。
		m.encapsulateMsgData(req.MsgData)
		switch req.MsgData.SessionType {
		case constant.SingleChatType:
			return m.sendMsgSingleChat(ctx, req)
		case constant.NotificationChatType:
			return m.sendMsgNotification(ctx, req)
		case constant.ReadGroupChatType:
			return m.sendMsgGroupChat(ctx, req)
		default:
			return nil, errs.ErrArgs.WrapMsg("unknown sessionType")
		}
	}
	return nil, errs.ErrArgs.WrapMsg("msgData is nil")
}

// 这里是 rpc server 端的实现逻辑
// 1003
func (m *msgServer) sendMsgGroupChat(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	// 这里 msgServer 作为 rpc-msg server 的实现, 其实没有被要求实现 sendMsgGroupChat 方法
	// sendMsgGroupChat 方法是作为实现接口的依赖方法
	if err = m.messageVerification(ctx, req); err != nil {
		prommetrics.GroupChatMsgProcessFailedCounter.Inc()
		return nil, err
	}
	if err = m.webhookBeforeSendGroupMsg(ctx, &m.config.WebhooksConfig.BeforeSendGroupMsg, req); err != nil {
		return nil, err
	}
	if err := m.webhookBeforeMsgModify(ctx, &m.config.WebhooksConfig.BeforeMsgModify, req); err != nil {
		return nil, err
	}
	err = m.MsgDatabase.MsgToMQ(ctx, conversationutil.GenConversationUniqueKeyForGroup(req.MsgData.GroupID), req.MsgData)
	if err != nil {
		return nil, err
	}
	if req.MsgData.ContentType == constant.AtText {
		go m.setConversationAtInfo(ctx, req.MsgData)
	}

	m.webhookAfterSendGroupMsg(ctx, &m.config.WebhooksConfig.AfterSendGroupMsg, req)
	prommetrics.GroupChatMsgProcessSuccessCounter.Inc()
	resp = &pbmsg.SendMsgResp{}
	resp.SendTime = req.MsgData.SendTime
	resp.ServerMsgID = req.MsgData.ServerMsgID
	resp.ClientMsgID = req.MsgData.ClientMsgID
	return resp, nil
}

func (m *msgServer) setConversationAtInfo(nctx context.Context, msg *sdkws.MsgData) {

	log.ZDebug(nctx, "setConversationAtInfo", "msg", msg)

	defer func() {
		if r := recover(); r != nil {
			log.ZPanic(nctx, "setConversationAtInfo Panic", errs.ErrPanic(r))
		}
	}()

	// template admin permission for next operation
	ctx := mcontext.NewCtx("@@@" + mcontext.GetOperationID(nctx))
	ctx = mcontext.WithOpUserIDContext(ctx, m.config.Share.IMAdminUserID[0])
	var atUserID []string

	conversation := &pbconversation.ConversationReq{
		ConversationID:   msgprocessor.GetConversationIDByMsg(msg),
		ConversationType: msg.SessionType,
		GroupID:          msg.GroupID,
	}
	memberUserIDList, err := m.GroupLocalCache.GetGroupMemberIDs(ctx, msg.GroupID)
	if err != nil {
		log.ZWarn(ctx, "GetGroupMemberIDs", err)
		return
	}

	tagAll := datautil.Contain(constant.AtAllString, msg.AtUserIDList...)
	if tagAll {

		memberUserIDList = datautil.DeleteElems(memberUserIDList, msg.SendID)

		atUserID = datautil.Single([]string{constant.AtAllString}, msg.AtUserIDList)

		if len(atUserID) == 0 { // just @everyone
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAll}
		} else { // @Everyone and @other people
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAllAtMe}
			atUserID = datautil.SliceIntersectFuncs(atUserID, memberUserIDList, func(a string) string { return a }, func(b string) string {
				return b
			})
			if err := m.conversationClient.SetConversations(ctx, atUserID, conversation); err != nil {
				log.ZWarn(ctx, "SetConversations", err, "userID", atUserID, "conversation", conversation)
			}
			memberUserIDList = datautil.Single(atUserID, memberUserIDList)
		}

		conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAll}
		if err := m.conversationClient.SetConversations(ctx, memberUserIDList, conversation); err != nil {
			log.ZWarn(ctx, "SetConversations", err, "userID", memberUserIDList, "conversation", conversation)
		}

		return
	}
	atUserID = datautil.SliceIntersectFuncs(msg.AtUserIDList, memberUserIDList, func(a string) string { return a }, func(b string) string {
		return b
	})
	conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtMe}

	if err := m.conversationClient.SetConversations(ctx, atUserID, conversation); err != nil {
		log.ZWarn(ctx, "SetConversations", err, atUserID, conversation)
	}
}

func (m *msgServer) sendMsgNotification(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	if err := m.MsgDatabase.MsgToMQ(ctx, conversationutil.GenConversationUniqueKeyForSingle(req.MsgData.SendID, req.MsgData.RecvID), req.MsgData); err != nil {
		return nil, err
	}
	resp = &pbmsg.SendMsgResp{
		ServerMsgID: req.MsgData.ServerMsgID,
		ClientMsgID: req.MsgData.ClientMsgID,
		SendTime:    req.MsgData.SendTime,
	}
	return resp, nil
}

func (m *msgServer) sendMsgSingleChat(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	// 管理员和通知类 ContentType 在这里直接通过；普通消息会检查黑名单，并在开启好友验证时检查好友关系。
	if err := m.messageVerification(ctx, req); err != nil {
		return nil, err
	}
	isSend := true
	// isNotNotification 缺省或为 true 时是普通消息；服务端明确写成 false 时才是通知。
	isNotification := msgprocessor.IsNotificationByMsg(req.MsgData)
	// 通知不受接收方「不接收 / 免打扰」设置影响。
	if !isNotification { // 如果不是通知
		// 不接收时 isSend 为 false；接收但不提醒时仍发送，只是关掉离线推送。
		isSend, err = m.modifyMessageByUserMessageReceiveOpt(
			ctx,
			req.MsgData.RecvID,
			// 生成单人对话的 conversationID
			conversationutil.GenConversationIDForSingle(req.MsgData.SendID, req.MsgData.RecvID),
			constant.SingleChatType,
			req,
		)
		if err != nil {
			return nil, err
		}
	}
	// 接收方拒收。没有错误码，调用方看到的是成功，但这条消息不会入队。
	if !isSend {
		prommetrics.SingleChatMsgProcessFailedCounter.Inc()
		return nil, nil
	} else {
		// 业务回调可以改消息；回调失败则中断，消息不会入队。
		if err := m.webhookBeforeMsgModify(ctx, &m.config.WebhooksConfig.BeforeMsgModify, req); err != nil {
			return nil, err
		}

		// 按单聊双方生成队列键。Seq 还没分配，由后续消费流程写入。
		if err := m.MsgDatabase.MsgToMQ(ctx, conversationutil.GenConversationUniqueKeyForSingle(req.MsgData.SendID, req.MsgData.RecvID), req.MsgData); err != nil {
			prommetrics.SingleChatMsgProcessFailedCounter.Inc()
			return nil, err
		}
		// 消息已经入队，这里的回调失败不再影响发送结果。
		m.webhookAfterSendSingleMsg(ctx, &m.config.WebhooksConfig.AfterSendSingleMsg, req)
		prommetrics.SingleChatMsgProcessSuccessCounter.Inc()
		return &pbmsg.SendMsgResp{
			ServerMsgID: req.MsgData.ServerMsgID,
			ClientMsgID: req.MsgData.ClientMsgID,
			SendTime:    req.MsgData.SendTime,
		}, nil
	}
}
