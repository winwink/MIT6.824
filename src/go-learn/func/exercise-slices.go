package main

import (
	"../../tour/pic"
	"fmt"
	//"math"
)

func Pic(dx, dy int) [][]uint8 {
	var result [][]uint8
	for i:=0;i<dy;i++{
		line := make([]uint8, dy)
		for j:=0;j<dy;j++ {
			line[j] = uint8(i)
		}
		result = append(result, line)
	}
	fmt.Println(result)
	return result
}

func main() {
	pic.Show(Pic)
}
