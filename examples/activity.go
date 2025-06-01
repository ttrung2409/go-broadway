package examples

import (
	"time"
)

type UserActivity struct {
	UserID    string
	Action    string
	Timestamp time.Time
	Payload   any
}

type UserSession struct {
	UserID              string
	IsLoggedIn          bool
	LastActivity        time.Time
	PageViews           int
	PurchaseCount       int
	TotalPurchaseAmount float64
}
