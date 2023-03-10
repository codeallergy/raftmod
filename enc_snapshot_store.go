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
	"crypto/sha256"
	"github.com/hashicorp/raft"
	"io"
)

type implEncryptedSnapshotStore struct {
	delegate  raft.SnapshotStore
	token     string
}

func NewEncryptedSnapshotStore(store raft.SnapshotStore, token string) (raft.SnapshotStore, error) {
	return &implEncryptedSnapshotStore{delegate: store, token: token}, nil
}

func (t *implEncryptedSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (sink raft.SnapshotSink, err error) {
	sink, err = t.delegate.Create(version, index, term, configuration, configurationIndex, trans)
	if err != nil {
		return
	}
	sessionKey := t.newSessionKey(index, term)
	sink, err = StreamEncrypter(sessionKey, sink)
	clean(sessionKey)
	return
}

func (t *implEncryptedSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return t.delegate.List()
}

func (t *implEncryptedSnapshotStore) Open(id string) (meta *raft.SnapshotMeta, source io.ReadCloser, err error) {
	meta, source, err = t.delegate.Open(id)
	if err != nil {
		return
	}
	sessionKey := t.newSessionKey(meta.Index, meta.Term)
	source, err = StreamDecrypter(sessionKey, source)
	clean(sessionKey)
	return
}

func (t *implEncryptedSnapshotStore) newSessionKey(index, term uint64) []byte {
	h := sha256.New()
	h.Write([]byte(t.token))
	return h.Sum(nil)
}

func clean(arr []byte) {
	n := len(arr)
	for i := 0; i < n; i++ {
		arr[i] = 0
	}
}


