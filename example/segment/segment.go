package main

import (
	"encoding/binary"

	"github.com/ljmsc/wal/segment"
)

func main() {

	s, err := segment.Open("./segment/segment_1")
	if err != nil {
		// handle error
		panic(err)
	}

	key := make([]byte, 0, 8)
	binary.PutUvarint(key, 42)

	if _, err := s.Write(key, []byte("mydata")); err != nil {
		// handle error
		panic(err)
	}

}
