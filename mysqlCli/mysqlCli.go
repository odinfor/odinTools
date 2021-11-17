package mysqlCli

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type mysqlClient struct {
	client   *gorm.DB
	host     string
	port     int
	username string
	password string
	database string
}

func MysqlObject(host, username, password, database string, port int) *mysqlClient {
	return &mysqlClient{
		host:     host,
		username: username,
		port:     port,
		password: password,
		database: database,
	}
}

func (m *mysqlClient) NewMysqlClient() (*gorm.DB, error) {
	var err error
	mysqlConnect := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		m.username, m.password, m.host, m.port, m.database,
	)
	m.client, err = gorm.Open("mysql", mysqlConnect)
	if err != nil {
		return nil, err
	} else {
		m.client.SingularTable(true) // 禁用复数表明,按照model结构体名称蛇形构造表名
		m.client.DB().SetMaxIdleConns(10)
		m.client.DB().SetMaxOpenConns(50)

		return m.client, nil
	}
}

func (m *mysqlClient) GetHost() string {
	return m.host
}

func (m *mysqlClient) GetPort() int {
	return m.port
}

func (m *mysqlClient) GetUsername() string {
	return m.username
}

func (m *mysqlClient) GetDatabase() string {
	return m.database
}
