package main

import "../../tour/tree"

// Walk 步进 tree t 将所有的值从 tree 发送到 channel ch。
func Walk(t *tree.Tree, ch chan int){
	if(t != nil) {
		ch <- t.Value
		Walk(t.Left, ch)
		Walk(t.Right, ch)
	}
}

// Same 检测树 t1 和 t2 是否含有相同的值。
// func Same(t1, t2 *tree.Tree) bool{

// }

func main() {
	ch := make(chan int)
	go Walk(tree.New(1), ch)

}
