package producer

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

var rmqProducer producerClientOfRocketmq

type producerClientOfRocketmq struct {
	producer rocketmq.Producer
}

type RmqProducerClient interface {
	SendSync(topic string, message string) (string, error)
	SendAsync(
		callbackFunc func(ctx context.Context, result *primitive.SendResult, err error), topic string, message ...string,
		) error
	Start() error
	Shutdown() error
}

func InitRocketmqProducerClient(nsAddr []string, accessKey string, secretKey string) error {
	var (
		err error
	)
	rmqProducer.producer, err = rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver(nsAddr)),
		producer.WithRetry(2),
		producer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
	)
	if err != nil {
		return err
	}

	return nil
}

//
// NewRocketmqProducer
// @Description: 获取生产者客户端
// @return *producerClientOfRocketmq
//
func NewRocketmqProducer() *producerClientOfRocketmq {
	return &rmqProducer
}

//
// SendSync
// @Description: 同步向topic中发送消息
// @receiver p
// @param topic rocketmq topic
// @param message 消息主体
// @return string 写入rocketmq message id
// @return error
//
func (p *producerClientOfRocketmq) SendSync(topic string, message ...string) (string, error) {
	var (
		res *primitive.SendResult
		err error
	)
	messageLen := len(message)

	switch messageLen {
	case 0:
		return "", fmt.Errorf("消息不能为空")
	case 1:
		if res, err = p.producer.SendSync(context.TODO(), &primitive.Message{
			Topic: topic,
			Body:  []byte(message[0]),
		}); err != nil {
			return "", err
		}
		return res.MsgID, nil
	default:
		var msg = make([]*primitive.Message, len(message))
		for _, v := range message {
			msg = append(msg, &primitive.Message{
				Topic: topic,
				Body:  []byte(v),
			})
		}
		if res, err = p.producer.SendSync(context.TODO(), msg...); err != nil {
			return "", err
		}
		return res.MsgID, nil
	}
}

//
// SendAsync
// @Description: 异步向topic发送消息
// @receiver p
// @param callbackFunc 回调函数
// @param topic rocketmq topic
// @param message 消息主体
// @return error
//
func (p *producerClientOfRocketmq) SendAsync(
	callbackFunc func(ctx context.Context, result *primitive.SendResult, err error), topic string, message ...string) error {
	messageLen := len(message)

	switch messageLen {
	case 0:
		return fmt.Errorf("消息不能为空")
	case 1:
		if err := p.producer.SendAsync(context.TODO(), callbackFunc, &primitive.Message{
			Topic: topic,
			Body:  []byte(message[0]),
		}); err != nil {
			return err
		}
		return nil
	default:
		var msg = make([]*primitive.Message, len(message))
		for _, v := range message {
			msg = append(msg, &primitive.Message{
				Topic: topic,
				Body:  []byte(v),
			})
		}
		if err := p.producer.SendAsync(context.TODO(), callbackFunc, msg...); err != nil {
			return err
		}
		return nil
	}
}

func (p *producerClientOfRocketmq) Start() error {
	if err := rmqProducer.producer.Start(); err != nil {
		return fmt.Errorf("rocketmq producer start fail, error: %v", err)
	}

	return nil
}

//
// Shutdown
// @Description: 关闭生产者
// @receiver p
// @return error
//
func (p *producerClientOfRocketmq) Shutdown() error {
	if err := p.producer.Shutdown(); err != nil {
		return fmt.Errorf("rocketmq producer shutdown fail, error: %v", err)
	}

	return nil
}
