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
	"crypto/rand"
	"crypto/tls"
	"errors"
	"github.com/hashicorp/raft"
	"net"
	"time"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// TCPStreamLayer implements StreamLayer interface for plain TCP.
type TCPStreamLayer struct {
	advertise     net.Addr
	listener      net.Listener
	tlsConfigOpt  *tls.Config // can be nil
}

func newTCPTransport(listener net.Listener,
	advertise net.Addr,
	tlsConfigOpt *tls.Config, // can be nil
	transportCreator func(stream raft.StreamLayer) *raft.NetworkTransport) (*raft.NetworkTransport, error) {

	// Create stream
	stream := &TCPStreamLayer{
		advertise:    advertise,
		listener:     listener,
		tlsConfigOpt: tlsConfigOpt,
	}

	// Verify that we have a usable advertise address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		return nil, errNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		return nil, errNotAdvertisable
	}

	// Create the network transport
	trans := transportCreator(stream)
	return trans, nil
}

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {

	if t.tlsConfigOpt != nil {

		tlsConf := &tls.Config{
			Rand:                        rand.Reader,
			Certificates:                t.tlsConfigOpt.Certificates,
			ClientCAs:                   t.tlsConfigOpt.ClientCAs,
			InsecureSkipVerify:          true,
		}

		d := net.Dialer{Timeout: timeout}
		return tls.DialWithDialer(&d, "tcp", string(address), tlsConf)
	} else {
		return net.DialTimeout("tcp", string(address), timeout)
	}

}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

