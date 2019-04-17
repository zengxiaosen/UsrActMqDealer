package main

import (
	"fmt"
)

func main() {
	//test encode
	msg1 := NewMessage(1, []byte("message codec test..."))
	data, err := Encode(msg1)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(msg1)
	//test decode
	//the first four bytes is size for socket read
	msg2, err := Decode(data[4:])
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("ID=%d, Data=%s", msg2.GetID(), string(msg2.GetData()))
}
