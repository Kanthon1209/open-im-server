package database

// 这里配置了各个 Collection (如果是 Mongo 实现的话) 的名字
const (
	BlackName               = "black"
	ConversationName        = "conversation"
	FriendName              = "friend"
	FriendVersionName       = "friend_version"
	FriendRequestName       = "friend_request"
	GroupName               = "group"
	GroupMemberName         = "group_member"
	GroupMemberVersionName  = "group_member_version"
	GroupJoinVersionName    = "group_join_version"
	ConversationVersionName = "conversation_version"
	GroupRequestName        = "group_request"
	LogName                 = "log"
	ObjectName              = "s3"
	UserName                = "user"
	SeqConversationName     = "seq"
	SeqUserName             = "seq_user"
)
