//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package wal

import (
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	sectorSize = 512
)

func pageSizefs(dir string) (int64, error) {
	stat := unix.Statfs_t{}
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, fmt.Errorf("can't read directory stats: %w", err)
	}
	return int64(stat.Bsize), nil
}

func pageSize(name string) (int64, error) {
	stat := unix.Stat_t{}
	if err := unix.Stat(name, &stat); err != nil {
		return 0, fmt.Errorf("can't read file stats: %w", err)
	}
	return int64(stat.Blksize), nil
}

func pages(name string) (int64, error) {
	stat := unix.Stat_t{}
	if err := unix.Stat(name, &stat); err != nil {
		return 0, fmt.Errorf("can't read file stats: %w", err)
	}

	return stat.Blocks / (int64(stat.Blksize) / sectorSize), nil
}
