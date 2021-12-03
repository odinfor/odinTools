package rocketmqCli

// http://120.79.202.23:2368/golangcao-zuo-xiao-xi-dui-lie-rocketmq/

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type producerClientOfRocketmq struct {
	producer rocketmq.Producer
}

type consumerClientOfRocketmq struct {
	pullConsumer rocketmq.PullConsumer
	pushConsumer rocketmq.PushConsumer
}

func NewRocketmqProducerClient(nsAddr []string, accessKey string, secretKey string) (*producerClientOfRocketmq, error) {
	var (
		producerClient producerClientOfRocketmq
		err error
	)
	producerClient.producer, err = rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver(nsAddr)),
		producer.WithRetry(2),
		producer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
	)
	if err != nil {
		return nil, err
	}
	return &producerClient, nil
}

type RocketmqProduce interface {
	Start() error
	ShoutDown() error
	SendSync(topic string, message string)
	SendAsync(topic string, message string)
}

func (p *producerClientOfRocketmq) Start() error {
	if err := p.producer.Start(); err != nil {
		return err
	}
	return nil
}

func (p *producerClientOfRocketmq) ShoutDown() error {
	if err := p.producer.Shutdown(); err != nil {
		return err
	}
	return nil
}

//
// SendSync
// @Description: 同步发送消息
// @receiver r
// @param topic: rocketmq topic
// @param message 消息内容
// @return *primitive.SendResult
// @return error
//
func (p *producerClientOfRocketmq) SendSync(topic string, message string) (*primitive.SendResult, error) {
	res, err := p.producer.SendSync(context.TODO(), &primitive.Message{
		Topic: topic,
		Body: []byte(message),
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

//
// SendAsync
// @Description: 异步发送消息示例,根据实际使用情况完成func
// @receiver r
// @param topic: rocketmq topic
// @param message
// @return error
//
func (p *producerClientOfRocketmq) SendAsync(topic string, message string) error {
	err := p.producer.SendAsync(
		context.Background(),
		func(ctx context.Context, result *primitive.SendResult, e error) {
			if e != nil {
				fmt.Printf("receive message error: %s\n", e)
			} else {
				fmt.Printf("send message success: result=%s\n", result.String())
			}
		},
		&primitive.Message{
			Topic: topic,
			Body: []byte(message),
		})
	return err
}

func NewRocketmqConsumerClient(nsAddr []string, groupName string, accessKey string, secretKey string) (*consumerClientOfRocketmq, error) {
	var (
		client consumerClientOfRocketmq
		err error
	)
	client.pullConsumer, err = rocketmq.NewPullConsumer(
		consumer.WithGroupName(groupName),
		consumer.WithRetry(2),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(nsAddr)),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
	)
	if err != nil {
		return nil, err
	}
	client.pushConsumer, err = rocketmq.NewPushConsumer(
		consumer.WithGroupName(groupName),
		consumer.WithRetry(2),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(nsAddr)),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
	)
	if err != nil {
		return nil, err
	}
	return &client, nil
}

func (c *consumerClientOfRocketmq) PullStart() error {
	if err := c.pullConsumer.Start(); err != nil {
		return err
	}
	return nil
}

func (c *consumerClientOfRocketmq) PullShoutDown() error {
	if err := c.pullConsumer.Shutdown(); err != nil {
		return err
	}
	return nil
}

func (c *consumerClientOfRocketmq) PushStart() error {
	if err := c.pushConsumer.Start(); err != nil {
		return err
	}
	return nil
}

func (c *consumerClientOfRocketmq) PushShoutDown() error {
	if err := c.pushConsumer.Shutdown(); err != nil {
		return err
	}
	return nil
}

func (c *consumerClientOfRocketmq) Pull(topic string, offset int64)  {
	c.pullConsumer.PullFrom(context.TODO(), primitive.MessageQueue{
		Topic: topic,
		BrokerName: "",
		QueueId: 0,
	}, offset, 10)
}

