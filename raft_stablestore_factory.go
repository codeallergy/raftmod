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
	"github.com/codeallergy/raftbadger"
	"github.com/codeallergy/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"reflect"
)

var StableStoreClass = reflect.TypeOf((*raft.StableStore)(nil)).Elem()

type implRaftStableStoreFactory struct {

	RaftStore     store.ManagedDataStore    `inject:"bean=raft-storage"`
	RaftConfPrefix string `value:"raft-storage.stable-prefix,default=conf"`
}

func RaftStableStoreFactory() glue.FactoryBean {
	return &implRaftStableStoreFactory{}
}

func (t *implRaftStableStoreFactory) Object() (object interface{}, err error) {

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
		return nil, errors.Errorf("managed data delegate 'raft-storage' must have badger backend")
	}

	return raftbadger.NewStableStore(db, []byte(t.RaftConfPrefix)), nil

}

func (t *implRaftStableStoreFactory) ObjectType() reflect.Type {
	return StableStoreClass
}

func (t *implRaftStableStoreFactory) ObjectName() string {
	return "raft-storage-stable"
}

func (t *implRaftStableStoreFactory) Singleton() bool {
	return true
}
