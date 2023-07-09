package pipeline

type Data struct {
	Timestamp string            `json:"timestamp"`
	LogLine   string            `json:"logLine"`
	Labels    map[string]string `json:"labels"`
	Parsed    map[string]string `json:"parsed"`
}
