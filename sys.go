package wal

import "syscall"

const (
	sectorSize = 512
)

func pageSizefs(dir string) (int64, error) {
	stat := syscall.Statfs_t{}
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Bsize), nil
}

func pageSize(name string) (int64, error) {
	stat := syscall.Stat_t{}
	if err := syscall.Stat(name, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Blksize), nil
}

func pages(name string) (int64, error) {
	stat := syscall.Stat_t{}
	if err := syscall.Stat(name, &stat); err != nil {
		return 0, err
	}

	return stat.Blocks / (int64(stat.Blksize) / sectorSize), nil
}
