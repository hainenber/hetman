package tailer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/hainenber/hetman/internal/tailer/state"
	"github.com/rs/zerolog"
)

type KafkaTailerInput struct {
	Brokers              []string
	Topics               []string
	Client               sarama.ConsumerGroup
	Ctx                  context.Context
	CancelFunc           context.CancelFunc
	Logger               zerolog.Logger
	ConsumerGroupHandler ConsumerGroupHandler
}

type KafkaTailerInputOption struct {
	brokers []string
	topics  []string
	logger  zerolog.Logger
}

type ConsumerGroupHandler struct {
	relayEventFunc func(string, *time.Time)
}

func (*ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.relayEventFunc(string(msg.Value), &msg.Timestamp)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func NewKafkaTailerInput(opt KafkaTailerInputOption) (*KafkaTailerInput, error) {
	config := sarama.NewConfig()
	config.Version = sarama.DefaultVersion
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Return.Errors = true

	client, err := sarama.NewConsumerGroup(opt.brokers, uuid.New().String(), config)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &KafkaTailerInput{
		Client:     client,
		Ctx:        ctx,
		CancelFunc: cancelFunc,
		Brokers:    opt.brokers,
		Topics:     opt.topics,
		Logger:     opt.logger,
	}, nil
}

func (k *KafkaTailerInput) Stop() error {
	k.CancelFunc()
	return nil
}

func (k *KafkaTailerInput) Run(relayEventFunc func(string, *time.Time)) {
	consumerGroupHandler := ConsumerGroupHandler{
		relayEventFunc: relayEventFunc,
	}
	for {
		if err := k.Client.Consume(k.Ctx, k.Topics, &consumerGroupHandler); err != nil {
			k.Logger.Error().Err(err).Msg("Error from consumer")
		}
		if k.Ctx.Err() != nil {
			return
		}
	}
}

func (k *KafkaTailerInput) GetEventSource() string {
	return fmt.Sprintf("topics=%s|brokers=%s", strings.Join(k.Topics, ","), strings.Join(k.Brokers, ","))
}

func (k *KafkaTailerInput) UpdateLastReadPosition(_ state.TailerState) (int64, error) {
	return int64(0), nil
}

func (k *KafkaTailerInput) GetLastReadPosition() (int64, error) {
	return int64(0), nil
}
