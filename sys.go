package wal

import "syscall"

func pageSize() (int64, error) {
	stat := syscall.Stat_t{}
	if err := syscall.Stat(".", &stat); err != nil {
		return 0, err
	}
	return int64(stat.Blksize), nil
}
