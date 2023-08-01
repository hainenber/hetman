package tailer

type KafkaTailerInput struct {
}

type KafkaTailerInputOption struct {
	brokers []string
	topics  []string
}

func NewKafkaTailerInput(opt KafkaTailerInputOption) (*KafkaTailerInput, error) {
	return nil, nil
}
