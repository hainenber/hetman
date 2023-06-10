package pipeline

type Data struct {
	Timestamp string
	LogLine   string
	Parsed    map[string]string
}
