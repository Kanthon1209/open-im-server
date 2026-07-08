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

package prommetrics

import (
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const commonPath = "/metrics"

var (
	baseCollector = []prometheus.Collector{
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	}
)

func Init(registry *prometheus.Registry, listener net.Listener, path string, handler http.Handler, cs ...prometheus.Collector) error {
	// 将所有的 Collector 注册进 prom registry
	registry.MustRegister(cs...)
	// 把 handler 和 http.ServeMux 绑定, 这个是专门用来实现路径到handler映射的
	srv := http.NewServeMux()
	srv.Handle(path, handler)
	return http.Serve(listener, srv)
}
