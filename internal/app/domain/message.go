package domain

type KafkaMessage struct {
	UserID   int
	Username string
	Event    string
}
