package examples

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/ttrung2409/go-broadway/broadway"
)

// ActivityProducer generates simulated user activities
type ActivityProducer struct {
	userIDs []string
	actions []string
}

func (p *ActivityProducer) Init(_ context.Context) {}

func (p *ActivityProducer) Clone() broadway.Producer {
	// Create 10 sample users
	userIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		userIDs[i] = fmt.Sprintf("user-%d", i+1)
	}

	// Define possible actions
	actions := []string{
		"login",
		"view_page",
		"click_button",
		"purchase",
		"logout",
	}

	return &ActivityProducer{
		userIDs: userIDs,
		actions: actions,
	}
}

// HandleDemand implements the basic Producer interface
func (p *ActivityProducer) HandleDemand(demand int, ctx context.Context) []*broadway.Message {
	result := make([]*broadway.Message, 0, demand)

	for i := 0; i < demand; i++ {
		userID := p.userIDs[rand.Intn(len(p.userIDs))]
		action := p.actions[rand.Intn(len(p.actions))]

		activity := UserActivity{
			UserID:    userID,
			Action:    action,
			Timestamp: time.Now(),
		}

		switch action {
		case "view_page":
			activity.Payload = map[string]string{"page": "product-" + strconv.Itoa(rand.Intn(100))}
		case "purchase":
			activity.Payload = map[string]interface{}{
				"product_id": "product-" + strconv.Itoa(rand.Intn(100)),
				"amount":     rand.Float64() * 100,
			}
		}

		result = append(result, broadway.NewMessage(activity))
	}

	return result
}
