package consumer

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

var rmqConsumer consumerClientOfRocketmq

type consumerClientOfRocketmq struct {
	pushConsumer rocketmq.PushConsumer
}

type RmqPushConsumerClient interface {
	Subscribe(
		topic string,
		doFunc func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error),
	) error
	Start() error
	Shutdown() error
}

//
// InitRocketmqConsumerClient
// @Description: 初始化订阅者消费端
// @param nsAddr rocketmq nameserver address
// @param groupName 消费者组
// @param accessKey rocketmq access key
// @param secretKey rocketmq secret key
// @return *consumerClientOfRocketmq consumer object
// @return error
//
func InitRocketmqConsumerClient(nsAddr []string, groupName string, accessKey string, secretKey string) (*consumerClientOfRocketmq, error) {
	var (
		err error
	)
	rmqConsumer.pushConsumer, err = rocketmq.NewPushConsumer(
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

	return &rmqConsumer, nil
}

//
// Subscribe
// @Description: 订阅消息
// @receiver c
// @param topic 订阅的rocketmq topic
// @param doFunc 接收到消息后的处理func
// @return error
//
func (c *consumerClientOfRocketmq) Subscribe(
	topic string,
	doFunc func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error),
	) error {

	if err := c.pushConsumer.Subscribe(topic, consumer.MessageSelector{}, doFunc); err != nil {
		return err
	}

	return nil
}

//
// Start
// @Description: 开始方法
// @receiver c
// @return error
//
func (c *consumerClientOfRocketmq) Start() error {
	if err := c.pushConsumer.Start(); err != nil {
		return fmt.Errorf("rocketmq consumer start fail, error: %v", err)
	}

	return nil
}

//
// Shutdown
// @Description: 关闭consumer方法
// @receiver c
// @return error
//
func (c *consumerClientOfRocketmq) Shutdown() error {
	if err := c.pushConsumer.Shutdown(); err != nil {
		return fmt.Errorf("rocketmq consumer shutdown fail, error: %v", err)
	}

	return nil
}