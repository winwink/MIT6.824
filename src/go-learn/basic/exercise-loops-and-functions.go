package main

import (
	"fmt";
	"strconv";
	"math"
)

func Sqrt(x float64) float64 {
	a := x / 2
	for !IsEqualEnough(a*a, x){
		k := a*a - x
		margin := k/(2*a)
		a = a - margin
		fmt.Println("a:"+strconv.FormatFloat(a, 'f', -1, 64))
	}
	fmt.Println("a*a:"+strconv.FormatFloat(a*a, 'f', -1, 64))
	return a
}

func IsEqualEnough(x float64, y float64) bool {
	if(math.Abs(x - y)<0.0000001){
		return true
	}
	return false
}

func main() {
	fmt.Println(Sqrt(2))
}
