package mongo

import (
	"testing"

	"github.com/kapetacom/sdk-go-config/providers"
	"github.com/stretchr/testify/assert"
)

func TestGetProtocol(t *testing.T) {
	t.Run("default protocol", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{}
		expected := "mongodb"
		actual := getProtocol(resInfo)
		assert.Equal(t, expected, actual)
	})
	t.Run("protocol specified in options", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{Options: map[string]interface{}{"protocol": "mongodb+srv"}}
		expected := "mongodb+srv"
		actual := getProtocol(resInfo)
		assert.Equal(t, expected, actual)
	})
}

func TestDBName(t *testing.T) {
	t.Run("default db name", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{}
		resourceName := "test"
		expected := "test"
		actual := getDBName(resInfo, resourceName)
		assert.Equal(t, expected, actual)
	})
	t.Run("db name specified in options", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{Options: map[string]interface{}{"dbName": "test_db"}}
		resourceName := "test"
		expected := "test_db"
		actual := getDBName(resInfo, resourceName)
		assert.Equal(t, expected, actual)
	})
}

func TestCreateConnectionStringProtocolMongodb(t *testing.T) {
	t.Run("create connection string with mongodb protocol", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{
			Host:        "localhost",
			Port:        "27017",
			Credentials: map[string]string{"username": "user", "password": "password"},
		}
		dbName := "test"
		urlWithPort := "mongodb://user:password@localhost:27017/test?authSource=admin&directConnection=true"

		config := &ConfigProviderMock{
			GetResourceInfoFunc: func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error) {
				return resInfo, nil
			},
		}
		actual, err := createConnectionString(config, dbName)
		assert.NoError(t, err)
		assert.Equal(t, urlWithPort, actual)
	})

	t.Run("verify that the mongodb+srv connection string doesn't have a port and a direct connection", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{
			Host:        "localhost",
			Port:        "27017",
			Options:     map[string]interface{}{"protocol": "mongodb+srv"},
			Credentials: map[string]string{"username": "user", "password": "password"},
		}
		dbName := "test"
		urlWithNoPort := "mongodb+srv://user:password@localhost/test?authSource=admin"

		config := &ConfigProviderMock{
			GetResourceInfoFunc: func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error) {
				return resInfo, nil
			},
		}
		actual, err := createConnectionString(config, dbName)
		assert.NoError(t, err)
		assert.Equal(t, urlWithNoPort, actual)
	})

	t.Run("verify that mongodb connection string contains directConnection", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{
			Host:        "localhost",
			Port:        "27017",
			Credentials: map[string]string{"username": "user", "password": "password"},
		}
		dbName := "test"
		urlWithPort := "mongodb://user:password@localhost:27017/test?authSource=admin&directConnection=true"

		config := &ConfigProviderMock{
			GetResourceInfoFunc: func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error) {
				return resInfo, nil
			},
		}
		actual, err := createConnectionString(config, dbName)
		assert.NoError(t, err)
		assert.Equal(t, urlWithPort, actual)
	})
}

func TestCreateConnectionString(t *testing.T) {
	t.Run("create connection string", func(t *testing.T) {
		config := &ConfigProviderMock{
			GetResourceInfoFunc: func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error) {
				return &providers.ResourceInfo{
					Host:        "localhost",
					Port:        "27017",
					Credentials: map[string]string{"username": "user", "password": "password"},
				}, nil
			},
		}
		resourceName := "test"
		expected := "mongodb://user:password@localhost:27017/test?authSource=admin&directConnection=true"
		actual, err := createConnectionString(config, resourceName)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}

type ConfigProviderMock struct {
	GetResourceInfoFunc func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error)
}

// Get implements providers.ConfigProvider.
func (*ConfigProviderMock) Get(path string) interface{} {
	panic("unimplemented")
}

// GetBlockDefinition implements providers.ConfigProvider.
func (*ConfigProviderMock) GetBlockDefinition() interface{} {
	panic("unimplemented")
}

// GetBlockReference implements providers.ConfigProvider.
func (*ConfigProviderMock) GetBlockReference() string {
	panic("unimplemented")
}

// GetInstanceForConsumer implements providers.ConfigProvider.
func (*ConfigProviderMock) GetInstanceForConsumer(resourceName string) (*providers.BlockInstanceDetails, error) {
	panic("unimplemented")
}

// GetInstanceHost implements providers.ConfigProvider.
func (*ConfigProviderMock) GetInstanceHost(instanceID string) (string, error) {
	panic("unimplemented")
}

// GetInstanceId implements providers.ConfigProvider.
func (*ConfigProviderMock) GetInstanceId() string {
	panic("unimplemented")
}

// GetInstanceOperator implements providers.ConfigProvider.
func (*ConfigProviderMock) GetInstanceOperator(instanceId string) (*providers.InstanceOperator, error) {
	panic("unimplemented")
}

// GetInstancesForProvider implements providers.ConfigProvider.
func (*ConfigProviderMock) GetInstancesForProvider(resourceName string) ([]*providers.BlockInstanceDetails, error) {
	panic("unimplemented")
}

// GetOrDefault implements providers.ConfigProvider.
func (*ConfigProviderMock) GetOrDefault(path string, defaultValue interface{}) interface{} {
	panic("unimplemented")
}

// GetProviderId implements providers.ConfigProvider.
func (*ConfigProviderMock) GetProviderId() string {
	panic("unimplemented")
}

// GetResourceInfo implements providers.ConfigProvider.
func (c *ConfigProviderMock) GetResourceInfo(resourceType string, portType string, resourceName string) (*providers.ResourceInfo, error) {
	return c.GetResourceInfoFunc(resourceType, portType, resourceName)
}

// GetServerHost implements providers.ConfigProvider.
func (*ConfigProviderMock) GetServerHost() (string, error) {
	panic("unimplemented")
}

// GetServerPort implements providers.ConfigProvider.
func (*ConfigProviderMock) GetServerPort(portType string) (string, error) {
	panic("unimplemented")
}

// GetServiceAddress implements providers.ConfigProvider.
func (*ConfigProviderMock) GetServiceAddress(serviceName string, portType string) (string, error) {
	panic("unimplemented")
}

// GetSystemId implements providers.ConfigProvider.
func (*ConfigProviderMock) GetSystemId() string {
	panic("unimplemented")
}
