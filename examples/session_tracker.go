package examples

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"github.com/ttrung2409/go-broadway/broadway"
)

type UserSessionTracker struct {
	userSessions map[string]*UserSession
}

func (t *UserSessionTracker) New() broadway.BatchProcessor {
	return &UserSessionTracker{
		userSessions: make(map[string]*UserSession),
	}
}

func (t *UserSessionTracker) Handle(messages []*broadway.Message, ctx context.Context) ([]*broadway.Message, error) {

	userId := messages[0].PartitionKey()

	session, ok := t.userSessions[userId]
	if !ok {
		session = &UserSession{
			UserID: userId,
		}

		t.userSessions[userId] = session
	}

	activities := lo.Map(messages, func(msg *broadway.Message, _ int) UserActivity {
		return msg.Payload.(UserActivity)
	})

	session.LastActivity = activities[len(activities)-1].Timestamp

	for _, activity := range activities {
		switch activity.Action {
		case "login":
			session.IsLoggedIn = true

		case "logout":
			session.IsLoggedIn = false

		case "view_page":
			session.PageViews++

		case "purchase":
			session.PurchaseCount++
			if data, ok := activity.Payload.(map[string]interface{}); ok {
				if amount, ok := data["amount"].(float64); ok {
					session.TotalPurchaseAmount += amount
				}
			}
		}
	}

	for userId, session := range t.userSessions {
		fmt.Printf("session updated: %s %+v\n", userId, session)
	}

	return messages, nil
}
