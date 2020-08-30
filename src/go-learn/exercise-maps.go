package main

import (
	"strings"
	"../tour/wc"
)

func WordCount(s string) map[string]int {
	words := strings.Fields(s)
	m := make(map[string]int)
	for _,word:= range words {
		count,ok := m[word]
		if(ok){
			m[word] = count + 1
		} else {
			m[word] = 1
		}
	}
	return m
}

func main() {
	wc.Test(WordCount)
}
