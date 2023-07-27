package forwarder

import (
	"github.com/IBM/sarama"
	"github.com/fatih/structs"
	"github.com/hainenber/hetman/internal/pipeline"
	"github.com/hainenber/hetman/internal/workflow"
	"github.com/samber/lo"
)

type KafkaOutput struct {
	producer sarama.SyncProducer
	settings workflow.KafkaForwarderConfig
}

type KafkaOutputSetting struct {
	Brokers []string
	Topic   string
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

func (k KafkaOutput) PreparePayload(forwardArgs ...pipeline.Data) (func() error, error) {
	// Convert event in pipeline.Data to sarama.Message format
	kafkaMessages := lo.Map(forwardArgs, func(item pipeline.Data, _ int) *sarama.ProducerMessage {
		return &sarama.ProducerMessage{
			Topic: k.settings.Topic,
			Value: sarama.StringEncoder(item.LogLine),
		}
	})

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
