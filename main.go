package main

import (
	"fmt"
	"os"
	"path/filepath"
)

type KV interface {
	// get, set, del
	Get(key []byte) (val []byte, ok bool)
	Set(key []byte, val []byte)
	Del(key []byte)
	// range query
	FindGreaterThan(key []byte) Iterator
	// ...
}

type Iterator interface {
	HasNext() bool
	Next() (key []byte, val []byte)
}

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync() // fsync
}

func randomInt() int {
	return int(os.Getpid()) + int(os.Getpid())
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err = fp.Write(data); err != nil {
		return err
	}
	if err = fp.Sync(); err != nil { // fsync the file contents
		return err
	}

	if err = os.Rename(tmp, path); err != nil {
		return err
	}

	// Open parent directory
	dir := filepath.Dir(path)
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()

	// Sync parent directory to ensure rename is durable
	err = dirFile.Sync()
	return err
}

// Why does renaming work?

// Filesystems keep a mapping from file names to file data, so replacing a file by renaming simply points the file name to the new data without touching the old data. This mapping is just a “directory”. The mapping is many-to-one, multiple names can reference the same file, even from different directories, this is the concept of “hard link”. A file with 0 references is automatically deleted.

// The atomicity and durability of rename() depends on directory updates. But unfortunately, updating a directory is only readers-writer atomic, it’s not power-loss atomic or durable. So SaveData2 is still incorrect.
