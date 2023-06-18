package pipeline

type Data struct {
	Timestamp string
	LogLine   string
	Labels    map[string]string
	Parsed    map[string]string
}
