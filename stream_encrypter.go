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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"io"
)

/**
STREAM ENCRYPTER

Warning: fast but modifies stream data
*/

type implStreamEncrypter struct {
	sink   raft.SnapshotSink
	stream cipher.Stream
}

func StreamEncrypter(sessionKey []byte, sink raft.SnapshotSink) (raft.SnapshotSink, error) {
	block, err := aes.NewCipher(sessionKey)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, block.BlockSize())
	_, err = rand.Read(iv)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, iv)
	n, err := sink.Write(iv)
	if err != nil {
		return nil, err
	}
	if len(iv) != n {
		return nil, errors.Errorf("i/o write error, written %d bytes whereas expected %d bytes", n, len(iv))
	}
	// clean IV
	for i := 0; i < n; i++ {
		iv[i] = 0
	}
	return &implStreamEncrypter{
		sink: sink,
		stream: stream,
	}, nil
}

func (t *implStreamEncrypter) Write(p []byte) (int, error) {
	t.stream.XORKeyStream(p, p)
	return t.sink.Write(p)
}

func (t *implStreamEncrypter) Close() error {
	return t.sink.Close()
}

func (t *implStreamEncrypter) ID() string {
	return t.sink.ID()
}

func (t *implStreamEncrypter) Cancel() error {
	return t.sink.Cancel()
}

/**
STREAM DECRYPTER

Warning: fast but modifies stream data
 */

type implStreamDecrypter struct {
	source io.ReadCloser
	stream cipher.Stream
}

func StreamDecrypter(sessionKey []byte, source io.ReadCloser) (io.ReadCloser, error) {
	block, err := aes.NewCipher(sessionKey)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, block.BlockSize())
	n, err := io.ReadFull(source, iv)
	if err != nil {
		return nil, err
	}
	if n < len(iv) {
		return nil, io.EOF
	}
	stream := cipher.NewCTR(block, iv)
	// clean IV
	for i := 0; i < n; i++ {
		iv[i] = 0
	}
	return &implStreamDecrypter{
		source: source,
		stream: stream,
	}, nil
}

func (t *implStreamDecrypter) Read(p []byte) (int, error) {
	n, err := t.source.Read(p)
	if n > 0 {
		t.stream.XORKeyStream(p[:n], p[:n])
		return n, err
	}
	return 0, io.EOF
}

func (t *implStreamDecrypter) Close() error {
	return t.source.Close()
}

