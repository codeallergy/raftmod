/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package raftmod

import (
	"crypto/tls"
	"github.com/codeallergy/glue"
	"github.com/codeallergy/sprint"
	"github.com/codeallergy/raftapi"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"net"
	"os"
	"strings"
	"time"
)

type implRaftServer struct {

	Properties      glue.Properties     `inject`
	Log             *zap.Logger         `inject`
	TlsConfig       *tls.Config         `inject:"optional"`
	NodeService     sprint.NodeService  `inject`

	LogStore           raft.LogStore      `inject`
	StableStore        raft.StableStore   `inject`
	FileSnapshotStore  *raft.FileSnapshotStore  `inject`

	// should be defined by application
	FSM      raft.FSM   `inject`

	RaftAddress  string          `value:"raft-server.listen-address,default="`
	MaxPool      int             `value:"raft-server.max-pool,default=3"`
	Timeout      time.Duration   `value:"raft-server.timeout,default=10s"`

	listener  net.Listener
	transport *raft.NetworkTransport

	raft      *raft.Raft

	running   atomic.Bool

}

func RaftServer() raftapi.RaftServer {
	return &implRaftServer{}
}

func (t *implRaftServer) PostConstruct() error {
	return nil
}

func (t *implRaftServer) BeanName() string {
	return "raft-server"
}

func (t *implRaftServer) GetStats(cb func(name, value string) bool) error {
	if t.raft != nil {
		for k, v := range t.raft.Stats() {
			cb(k, v)
		}
	}
	return nil
}

func (t *implRaftServer) Bind() (err error) {

	if t.RaftAddress == "" {
		t.Log.Warn("RaftAddressEmpty", zap.String("prop", "raft.listen-address"))
		return nil
	}

	parts := strings.Split(t.RaftAddress, ":")
	if parts[0] == "" {
		ipAddr, err := LocalIP()
		if err == nil {
			parts[0] = ipAddr.String()
			t.RaftAddress = strings.Join(parts, ":")
		}
	}

	t.listener, err = net.Listen("tcp", t.RaftAddress)
	if err != nil {
		return errors.Errorf("bind failed on '%s', %v", t.RaftAddress, err)
	}

	advertise, err := net.ResolveTCPAddr("tcp", t.listener.Addr().String())
	if err != nil {
		return errors.Errorf("tcp address resolve '%s', %v", t.listener.Addr().String(), err)
	}

	t.transport, err = newTCPTransport(t.listener, advertise, t.TlsConfig, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransport(stream, t.MaxPool, t.Timeout, os.Stderr)
	})
	if err != nil {
		return errors.Errorf("raft transport creation error for address '%s', %v", advertise.String(), err)
	}

	return nil
}

func (t *implRaftServer) Active() bool {
	return t.running.Load()
}

func (t *implRaftServer) Transport() (raft.Transport, bool) {
	return t.transport, t.running.Load()
}

func (t *implRaftServer) Raft() (*raft.Raft, bool) {
	return t.raft, t.running.Load()
}

func (t *implRaftServer) ListenAddress() net.Addr {
	if t.listener != nil {
		return t.listener.Addr()
	} else {
		return EmptyAddr{}
	}
}

func (t *implRaftServer) Serve() (err error) {

	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			case string:
				err = errors.New(v)
			default:
				err = errors.Errorf("%v", v)
			}
		}
	}()

	t.Log.Info("RaftServerServe", zap.String("addr", t.RaftAddress), zap.Bool("tls", t.TlsConfig != nil))

	t.running.Store(true)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(t.NodeService.NodeIdHex())

	t.raft, err = raft.NewRaft(config, t.FSM, t.LogStore, t.StableStore, t.FileSnapshotStore, t.transport)
	if err != nil {
		return err
	}

	t.running.Store(true)
	return nil
}

func (t *implRaftServer) Stop() {
	t.running.Store(false)
	if t.running.CompareAndSwap(true, false) {
		if t.raft != nil {
			t.raft.Shutdown()
		}
		if t.transport != nil {
			t.transport.Close()
		}
		if t.listener != nil {
			t.listener.Close()
		}
	}
}

func (t *implRaftServer) Destroy() error {
	t.Stop()
	return nil
}


