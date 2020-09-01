package main

import (
	"fmt"
	"time"
)


func fibonacci(c, quit chan int) {
	x, y := 0, 1
	for {
		select {
		case c <- x:
			x, y = y, x+y
			// fmt.Println("input", x , y)
		case <-quit:
			fmt.Println("quit")
			return
		}
	}
}

func main() {
	c := make(chan int)
	quit := make(chan int)
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(1000 * time.Millisecond)
			fmt.Println("output1",<-c)
		}
		quit <- 0
	}()
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(1000 * time.Millisecond)
			fmt.Println("output2",<-c)
		}
		quit <- 0
	}()
	fibonacci(c, quit)
}
