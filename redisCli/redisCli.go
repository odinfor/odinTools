package redisCli

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type redisClient struct {
	client   *redis.Client
	host     string
	port     int
	password string
	db       int
}

func InitRedisConfig(host, password string, port, db int) *redisClient {
	return &redisClient{
		host: host,
		port: port,
		password: password,
		db: db,
	}
}

func (r *redisClient) NewRedisClient() (*redis.Client, error) {
	var err error
	r.client = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s/%d", r.host, r.port),
		Password: r.password,
		DB: r.db,
	})

	// 测试连接
	if err = r.client.Set(ctx, "testConnectKey", 0, 5).Err(); err != nil {
		return nil, err
	}
	return r.client, nil
}