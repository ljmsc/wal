package main

import (
	"encoding/binary"

	"github.com/ljmsc/wal/pouch"
)

func main() {

	p, err := pouch.Open("./pouch/pouch_1")
	if err != nil {
		// handle error
		panic(err)
	}

	key := make([]byte, 0, 8)
	binary.PutUvarint(key, 42)

	if _, err := p.Write(key, []byte("mydata")); err != nil {
		// handle error
		panic(err)
	}

}
