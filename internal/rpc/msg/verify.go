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
	"math/rand"
	"strconv"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/servererrs"
	"github.com/openimsdk/tools/utils/datautil"
	"github.com/openimsdk/tools/utils/encrypt"
	"github.com/openimsdk/tools/utils/timeutil"

	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
)

var ExcludeContentType = []int{constant.HasReadReceipt}

type Validator interface {
	validate(pb *msg.SendMsgReq) (bool, int32, string)
}

type MessageRevoked struct {
	RevokerID                   string `json:"revokerID"`
	RevokerRole                 int32  `json:"revokerRole"`
	ClientMsgID                 string `json:"clientMsgID"`
	RevokerNickname             string `json:"revokerNickname"`
	RevokeTime                  int64  `json:"revokeTime"`
	SourceMessageSendTime       int64  `json:"sourceMessageSendTime"`
	SourceMessageSendID         string `json:"sourceMessageSendID"`
	SourceMessageSenderNickname string `json:"sourceMessageSenderNickname"`
	SessionType                 int32  `json:"sessionType"`
	Seq                         uint32 `json:"seq"`
}

func (m *msgServer) messageVerification(ctx context.Context, data *msg.SendMsgReq) error {
	switch data.MsgData.SessionType {
	case constant.SingleChatType:
		if datautil.Contain(data.MsgData.SendID, m.config.Share.IMAdminUserID...) {
			return nil
		}
		if data.MsgData.ContentType <= constant.NotificationEnd &&
			data.MsgData.ContentType >= constant.NotificationBegin {
			return nil
		}
		if err := m.webhookBeforeSendSingleMsg(ctx, &m.config.WebhooksConfig.BeforeSendSingleMsg, data); err != nil {
			return err
		}
		black, err := m.FriendLocalCache.IsBlack(ctx, data.MsgData.SendID, data.MsgData.RecvID)
		if err != nil {
			return err
		}
		if black {
			return servererrs.ErrBlockedByPeer.Wrap()
		}
		if m.config.RpcConfig.FriendVerify {
			friend, err := m.FriendLocalCache.IsFriend(ctx, data.MsgData.SendID, data.MsgData.RecvID)
			if err != nil {
				return err
			}
			if !friend {
				return servererrs.ErrNotPeersFriend.Wrap()
			}
			return nil
		}
		return nil
	case constant.ReadGroupChatType:
		groupInfo, err := m.GroupLocalCache.GetGroupInfo(ctx, data.MsgData.GroupID)
		if err != nil {
			return err
		}
		if groupInfo.Status == constant.GroupStatusDismissed &&
			data.MsgData.ContentType != constant.GroupDismissedNotification {
			return servererrs.ErrDismissedAlready.Wrap()
		}
		if groupInfo.GroupType == constant.SuperGroup {
			return nil
		}

		if datautil.Contain(data.MsgData.SendID, m.config.Share.IMAdminUserID...) {
			return nil
		}
		if data.MsgData.ContentType <= constant.NotificationEnd &&
			data.MsgData.ContentType >= constant.NotificationBegin {
			return nil
		}
		memberIDs, err := m.GroupLocalCache.GetGroupMemberIDMap(ctx, data.MsgData.GroupID)
		if err != nil {
			return err
		}
		if _, ok := memberIDs[data.MsgData.SendID]; !ok {
			return servererrs.ErrNotInGroupYet.Wrap()
		}

		groupMemberInfo, err := m.GroupLocalCache.GetGroupMember(ctx, data.MsgData.GroupID, data.MsgData.SendID)
		if err != nil {
			if errs.ErrRecordNotFound.Is(err) {
				return servererrs.ErrNotInGroupYet.WrapMsg(err.Error())
			}
			return err
		}
		if groupMemberInfo.RoleLevel == constant.GroupOwner {
			return nil
		} else {
			if groupMemberInfo.MuteEndTime >= time.Now().UnixMilli() {
				return servererrs.ErrMutedInGroup.Wrap()
			}
			if groupInfo.Status == constant.GroupStatusMuted && groupMemberInfo.RoleLevel != constant.GroupAdmin {
				return servererrs.ErrMutedGroup.Wrap()
			}
		}
		return nil
	default:
		return nil
	}
}

// encapsulateMsgData 在按 SessionType 分发之前执行。单聊、群聊和通知都会先经过这里。
func (m *msgServer) encapsulateMsgData(msg *sdkws.MsgData) {
	// 服务端消息 ID 每次都重新生成，由发送方 ID、当前时间和随机数做 MD5。
	msg.ServerMsgID = GetMsgID(msg.SendID)
	// 调用方已经填了发送时间就保留，没填才写成当前毫秒时间。
	if msg.SendTime == 0 {
		msg.SendTime = timeutil.GetCurrentTimestampByMill()
	}
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
		fallthrough
	case constant.AtText:
		fallthrough
	case constant.Merger:
		fallthrough
	case constant.Card:
		fallthrough
	case constant.Location:
		fallthrough
	case constant.Custom:
		fallthrough
	case constant.Quote:
		// 普通消息不改开关。
	case constant.Revoke:
		// 撤回不增加未读，也不做离线推送。
		// msg.Options 是 map[string]bool 是开关表
		datautil.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false) // 不计入未读
		datautil.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false) // 不计入离线推送
	case constant.HasReadReceipt:
		// 已读回执同样不增加未读、不做离线推送，也不更新会话。
		datautil.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false) // 
		datautil.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)
	case constant.Typing:
		// 正在输入只需要实时送达，不入库、不同步多端、不更新会话。
		datautil.SetSwitchFromOptions(msg.Options, constant.IsHistory, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsPersistent, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsSenderSync, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)
		datautil.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)
	}
}

func GetMsgID(sendID string) string {
	t := timeutil.GetCurrentTimeFormatted()
	return encrypt.Md5(t + "-" + sendID + "-" + strconv.Itoa(rand.Int()))
}

// modifyMessageByUserMessageReceiveOpt 只处理普通单聊。userID 是接收方。
// 返回的 bool 就是调用方的 isSend：true 继续入队，false 表示接收方拒收。
func (m *msgServer) modifyMessageByUserMessageReceiveOpt(ctx context.Context, userID, conversationID string, sessionType int, pb *msg.SendMsgReq) (bool, error) {
	// 先看接收方的全局接收设置：0 正常接收，1 不接收，2 接收但不提醒。
	opt, err := m.UserLocalCache.GetUserGlobalMsgRecvOpt(ctx, userID)
	if err != nil {
		return false, err
	}
	switch opt {
	case constant.ReceiveMessage:
		// 全局正常接收，继续看这个单聊会话自己的设置。
	case constant.NotReceiveMessage:
		// 全局拒收，消息不入队。
		return false, nil
	case constant.ReceiveNotNotifyMessage:
		// 全局免打扰：消息仍发送，但关掉离线推送，不再看会话设置。
		if pb.MsgData.Options == nil {
			pb.MsgData.Options = make(map[string]bool, 10)
		}
		datautil.SetSwitchFromOptions(pb.MsgData.Options, constant.IsOfflinePush, false)
		return true, nil
	}
	// 这一对用户的单聊会话设置。没有单独设置过就按正常接收。
	singleOpt, err := m.ConversationLocalCache.GetSingleConversationRecvMsgOpt(ctx, userID, conversationID)
	if errs.ErrRecordNotFound.Is(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	switch singleOpt {
	case constant.ReceiveMessage:
		return true, nil
	case constant.NotReceiveMessage:
		// 会话拒收时，已读回执仍然发送，其他消息不入队。
		if datautil.Contain(int(pb.MsgData.ContentType), ExcludeContentType...) {
			return true, nil
		}
		return false, nil
	case constant.ReceiveNotNotifyMessage:
		// 这个会话免打扰：消息仍发送，关掉离线推送。
		if pb.MsgData.Options == nil {
			pb.MsgData.Options = make(map[string]bool, 10)
		}
		datautil.SetSwitchFromOptions(pb.MsgData.Options, constant.IsOfflinePush, false)
		return true, nil
	}
	return true, nil
}
