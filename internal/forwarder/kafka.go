package forwarder

import (
	"github.com/IBM/sarama"
	"github.com/fatih/structs"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/workflow"
)

type KafkaOutput struct {
	producer sarama.SyncProducer
	settings workflow.KafkaForwarderConfig
}

type KafkaOutputSetting struct {
	Brokers []string
	Topics  []string
}

func NewKafkaOutput(setting KafkaOutputSetting) (*KafkaOutput, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(setting.Brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaOutput{
		producer: producer,
		settings: workflow.KafkaForwarderConfig(setting),
	}, nil
}

func (k KafkaOutput) SendEvents(forwardArgs ...pipeline.Data) (func() error, error) {
	// Convert event in pipeline.Data to sarama.Message format
	var kafkaMessages []*sarama.ProducerMessage
	for _, topic := range k.settings.Topics {
		for _, item := range forwardArgs {
			kafkaMessages = append(kafkaMessages, &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(item.LogLine),
			})
		}
	}

	innerForwarderFunc := func() error {
		if err := k.producer.SendMessages(kafkaMessages); err != nil {
			return err
		}
		return nil
	}
	return innerForwarderFunc, nil
}

func (k KafkaOutput) GetSettings() map[string]interface{} {
	return structs.Map(k.settings)
}
