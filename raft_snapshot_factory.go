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
	"fmt"
	"github.com/codeallergy/glue"
	"github.com/codeallergy/sprint"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"reflect"
)

var FileSnapshotStoreClass = reflect.TypeOf((*raft.FileSnapshotStore)(nil))

type implRaftSnapshotFactory struct {

	Application sprint.Application `inject`
	Properties  glue.Properties `inject`

	RetainSnapshotCount     int          `value:"raft-snapshot.retain-count,default=2"`

	DataDir           string       `value:"application.data.dir,default="`
	DataDirPerm       os.FileMode  `value:"application.perm.data.dir,default=-rwxrwx---"`
	DataFilePerm      os.FileMode  `value:"application.perm.data.file,default=-rw-rw-r--"`
}

func RaftSnapshotFactory() glue.FactoryBean {
	return &implRaftSnapshotFactory{}
}

func (t *implRaftSnapshotFactory) Object() (object interface{}, err error) {

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

	dataDir := t.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(t.Application.ApplicationDir(), "db")

		if err := createDirIfNeeded(dataDir, t.DataDirPerm); err != nil {
			return nil, err
		}

		dataDir = filepath.Join(dataDir, t.Application.Name())
	}

	if err := createDirIfNeeded(dataDir, t.DataDirPerm); err != nil {
		return nil, err
	}

	snapshotsFolder := filepath.Join(dataDir, "raft-snapshot")

	if err := createDirIfNeeded(snapshotsFolder, t.DataDirPerm); err != nil {
		return nil, err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(snapshotsFolder, t.RetainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft snapshots '%s' creation error, %v", snapshotsFolder, err)
	}

	return snapshots, nil
}

func (t *implRaftSnapshotFactory) ObjectType() reflect.Type {
	return FileSnapshotStoreClass
}

func (t *implRaftSnapshotFactory) ObjectName() string {
	return "raft-snapshot"
}

func (t *implRaftSnapshotFactory) Singleton() bool {
	return true
}


