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
	"github.com/codeallergy/glue"
	"github.com/codeallergy/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/codeallergy/raftbadger"
	"reflect"
)

var LogStoreClass = reflect.TypeOf((*raft.LogStore)(nil)).Elem()

type implRaftLogStoreFactory struct {

	RaftStore     store.ManagedDataStore    `inject:"bean=raft-storage"`
	RaftLogPrefix string `value:"raft-storage.log-prefix,default=log"`

}

func RaftLogStoreFactory() glue.FactoryBean {
	return &implRaftLogStoreFactory{}
}

func (t *implRaftLogStoreFactory) Object() (object interface{}, err error) {

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

	db, ok := t.RaftStore.Instance().(*badger.DB)
	if !ok {
		return nil, errors.New("managed data store 'raft-storage' must have badger backend")
	}

	return raftbadger.NewLogStore(db, []byte(t.RaftLogPrefix)), nil

}

func (t *implRaftLogStoreFactory) ObjectType() reflect.Type {
	return LogStoreClass
}

func (t *implRaftLogStoreFactory) ObjectName() string {
	return "raft-storage-log"
}

func (t *implRaftLogStoreFactory) Singleton() bool {
	return true
}
