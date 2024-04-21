package main

import (
	"fmt"

	"github.com/emirpasic/gods/trees/redblacktree"
)

func main() {
	// ticker := time.NewTicker(2 * time.Second)
	// go func() {
	// 	for {
	// 		val := <-ticker.C
	// 		fmt.Println("val : ", val)
	// 	}
	// }()

	rbt := redblacktree.NewWithStringComparator()
	rbt.Put("e", "f")
	rbt.Put("a", "f")
	rbt.Put("c", "f")
	rbt.Put("b", "f")
	rbt.Put("i", "f")
	rbt.Put("h", "f")

	// time.Sleep(10 * time.Second)
	iterator := rbt.Iterator()

	for iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()

		fmt.Println("key: ", key)
		fmt.Println("val : ", value)
	}
}
