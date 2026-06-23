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

package cmd

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/internal/msggateway"
	"github.com/openimsdk/open-im-server/v3/version"

	"github.com/openimsdk/tools/system/program"
	"github.com/spf13/cobra"
)

type MsgGatewayCmd struct {
	*RootCmd         // 直接嵌入的 RootCmd, 会将 RootCmd 的方法提升到外层
	ctx              context.Context
	configMap        map[string]any
	msgGatewayConfig *msggateway.Config
}

func NewMsgGatewayCmd() *MsgGatewayCmd {
	var msgGatewayConfig msggateway.Config
	ret := &MsgGatewayCmd{msgGatewayConfig: &msgGatewayConfig}
	ret.configMap = map[string]any{
		OpenIMMsgGatewayCfgFileName: &msgGatewayConfig.MsgGateway,
		ShareFileName:               &msgGatewayConfig.Share,
		RedisConfigFileName:         &msgGatewayConfig.RedisConfig,
		WebhooksConfigFileName:      &msgGatewayConfig.WebhooksConfig,
		DiscoveryConfigFilename:     &msgGatewayConfig.Discovery,
	}
	ret.RootCmd = NewRootCmd(program.GetProcessName(), WithConfigMap(ret.configMap))
	ret.ctx = context.WithValue(context.Background(), "version", version.Version)
	// 在这里绑定了 命令解析后运行的逻辑
	ret.Command.RunE = func(cmd *cobra.Command, args []string) error {
		return ret.runE()
	}
	return ret
}

func (m *MsgGatewayCmd) Exec() error {
	return m.Execute() // 这里 Execute 就是 RootCmd 的方法
	// 因为嵌入了 MsgGatewayCmd, 且 MsgGateway 没有对同名方法进行重写, 并且, 这个方法是 public 方法
	// 所以, 会方法提升到外层
}

func (m *MsgGatewayCmd) runE() error {
	// 这里会把 CMD 从配置文件中映射过的 Config 结构体传入到 msggateway 的启动函数
	return msggateway.Start(m.ctx, m.Index(), m.msgGatewayConfig)
}
