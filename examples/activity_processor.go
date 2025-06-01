package examples

import (
	"context"

	"github.com/ttrung2409/go-broadway/broadway"
)

type ActivityProcessor struct {
}

func (p *ActivityProcessor) New() broadway.MessageProcessor {
	return &ActivityProcessor{}
}

func (p *ActivityProcessor) Handle(message *broadway.Message, ctx context.Context) (*broadway.Message, error) {

	message.Batcher = "session_tracker"
	return message, nil
}
