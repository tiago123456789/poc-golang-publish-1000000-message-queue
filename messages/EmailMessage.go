package messages

type EmailMessage struct {
	To      string `json:"to"`
	Content string `json:"content"`
}
