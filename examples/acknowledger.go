package examples

import (
	"fmt"

	"github.com/ttrung2409/go-broadway/broadway"
)

func ack(messages []*broadway.Message, err error) {
	if err != nil {
		fmt.Printf("%d messages failed\n:", len(messages))

	} else {
		fmt.Printf("%d messages processed successfully\n:", len(messages))
	}
}
