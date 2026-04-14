package main

import (
	"fmt"
	"path/filepath"
	"time"

	"lns.com/bptree/bptree"
)

func test1() {
	path := filepath.Join("test.db")

	tree, err := bptree.OpenPersistentBPTree(path, 1024, 1024)
	if err != nil {
		panic(err)
	}
	defer tree.Close()

	key_num := 100000000
	keys := make([][]byte, key_num)
	vals := make([][]byte, key_num)
	for i := range key_num {
		keys[i] = []byte(fmt.Sprintf("key_%06d", i))
		vals[i] = []byte(fmt.Sprintf("val_%06d", i))
	}

	start := time.Now()
	for i := range key_num {
		if err := tree.Insert(keys[i], vals[i]); err != nil {
			panic(err)
		}

		if (i+1)%10000000 == 0 {
			fmt.Printf("inserted %d keys, time used is %d ms.\n", i+1, time.Since(start).Milliseconds())
		}
	}

	fmt.Printf("time used is %d ms.\n", time.Since(start).Milliseconds())
	tree.Close()
}

func main() {
	fmt.Println("hello world")
	test1()
}
