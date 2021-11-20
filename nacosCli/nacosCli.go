package nacosCli

import (
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

type nacosClient struct {
	clientConfig  constant.ClientConfig
	serverConfigs []constant.ServerConfig
	namingClient  *naming_client.INamingClient
	configClient  *config_client.IConfigClient
}

// NewNacosClient
// @Description: nacos客户端,部分参数默认配置,需要更改自行调用对应方法
// @param namespaceId: nacos 命名空间
// @param username nacos用户名
// @param password nacos密码
// @param ipAddr nacos 集群ip
// @return *nacosClient
//
func NewNacosClient(namespaceId string, username string, password string, ipAddr []string) *nacosClient {
	var serverConfigs []constant.ServerConfig
	for _, v := range ipAddr {
		var c constant.ServerConfig
		c.IpAddr = v
		c.Port = 8848
		c.Scheme = "http"

		serverConfigs = append(serverConfigs, c)
	}
	return &nacosClient{
		clientConfig: constant.ClientConfig{
			NamespaceId:         namespaceId,
			TimeoutMs:           5000,
			NotLoadCacheAtStart: true,
			LogDir:              "E:\\odin\\foodi\\registerCenter\\nacos\\log",
			CacheDir:            "E:\\odin\\foodi\\registerCenter\\nacos\\cache",
			Username:            username,
			Password:            password,
			MaxAge:              7,
		},
		serverConfigs: serverConfigs,
	}
}

// SetLogDir
// @Description: 修改nacos 日志存储地址
// @receiver n
// @param path
//
func (n *nacosClient) SetLogDir(path string) {
	n.clientConfig.LogDir = path
}

// SetCacheDir
// @Description: 修改nacos缓存存储地址
// @receiver n
// @param path
//
func (n *nacosClient) SetCacheDir(path string) {
	n.clientConfig.CacheDir = path
}

// SetMaxAge
// @Description: 修改nacos日志保存天数,默认24h切割
// @receiver n
// @param path
//
func (n *nacosClient) SetMaxAge(num int64) {//

	n.clientConfig.MaxAge = num
}

// SetTimeout
// @Description: 修改nacos连接超时时间
// @receiver n
// @param path
//
func (n *nacosClient) SetTimeout(num uint64) {
	n.clientConfig.TimeoutMs = num
}

// CreateNamingClient
// @Description: 创建连接客户端
// @receiver n
// @param path
//
func (n *nacosClient) CreateNamingClient() error {
	namingClient, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  &n.clientConfig,
		ServerConfigs: n.serverConfigs,
	})
	if err != nil {
		return err
	}
	n.namingClient = &namingClient
	return nil
}

// CreateConfigClient
// @Description: 创建配置客户端
// @receiver n
// @return error
//
func (n *nacosClient) CreateConfigClient() error {//
	configClient, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &n.clientConfig,
		ServerConfigs: n.serverConfigs,
	})
	if err != nil {
		return err
	}
	n.configClient = &configClient
	return nil
}

// RegisterInstance
// @Description: 服务注册到nacos
// @receiver n
// @param nacosIp
// @param serviceName
// @param groupName
// @param clusterName
// @return error
//
func (n *nacosClient) RegisterInstance(nacosIp string, serviceName string, groupName string, clusterName string) error {
	client := *n.namingClient
	success, err := client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          nacosIp,
		Port:        8848,
		ServiceName: serviceName,
		GroupName:   groupName,
		Weight:      10,
		ClusterName: clusterName,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	if !success {
		return err
	}
	return nil
}

// GetService
// @Description: 获取服务信息
// @receiver n
// @param serviceName 服务名称
// @param cluster 集群名称
// @param groupName 组名
// @return model.Service 服务信息
// @return error
//
func (n *nacosClient) GetService(serviceName string, cluster []string, groupName string) (model.Service, error) {
	client := *n.namingClient
	services, err := client.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		Clusters:    cluster,
		GroupName:   groupName,
	})
	if err != nil {
		return model.Service{}, err
	}
	return services, nil
}

// GetConfig
// @Description: 获取配置,建议配置用json,可直接绑定到结构体
// @receiver n
// @param dataId 配置的data id
// @param group
// @return string
// @return error
//
func (n *nacosClient) GetConfig(dataId string, group string) (string, error) {
	client := *n.configClient
	content, err := client.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		return "", err
	}
	return content, nil
}
