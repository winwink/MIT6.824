package main

import (
	"io"
	"os"
	"strings"
)

type rot13Reader struct {
	r io.Reader
}

func (rot rot13Reader) Read(b []byte) (int, error){
	reader := rot.r
	v, err := reader.Read(b)
	for i:= 0;i<len(b);i++{
		b[i] = RotConvert(b[i])
	}
	return v, err
}

func RotConvert(input byte) byte{
	if((input >= 'N' && input <='Z') || (input >='n' && input <='z')){
		return input-13
	} else if((input >='A' && input <='M')||(input >='a' && input <='m')){
		return input + 13
	}
	return input
}

func main() {
	s := strings.NewReader("Lbh penpxrq gur pbqr!")
	r := rot13Reader{s}
	io.Copy(os.Stdout, &r)
}
