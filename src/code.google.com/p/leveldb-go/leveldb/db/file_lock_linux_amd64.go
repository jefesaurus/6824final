// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

// lockCloser hides all of an os.File's methods, except for Close.
type lockCloser struct {
	f *os.File
}

func (l lockCloser) Close() error {
	return l.f.Close()
}

func (defFS) Lock(name string) (io.Closer, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	// This type matches C's "struct flock" defined in /usr/include/bits/fcntl.h.
	// TODO: move this into the standard syscall package.
	k := struct {
		Type   uint32
		Whence uint32
		Start  uint64
		Len    uint64
		Pid    uint32
	}{
		Type:   syscall.F_WRLCK,
		Whence: uint32(os.SEEK_SET),
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
		Pid:    uint32(os.Getpid()),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_SETLK), uintptr(unsafe.Pointer(&k)))
	if errno != 0 {
		f.Close()
		return nil, errno
	}
	return lockCloser{f}, nil
}
