package mongo

import (
	"testing"

	config "github.com/kapetacom/sdk-go-config"
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

		config := &config.ConfigProviderMock{
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

		config := &config.ConfigProviderMock{
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

		config := &config.ConfigProviderMock{
			GetResourceInfoFunc: func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error) {
				return resInfo, nil
			},
		}
		actual, err := createConnectionString(config, dbName)
		assert.NoError(t, err)
		assert.Equal(t, urlWithPort, actual)
	})

	t.Run("verify that mongodb connection string contains ssl true", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{
			Host:        "localhost",
			Port:        "27017",
			Credentials: map[string]string{"username": "user", "password": "password"},
			Options:     map[string]interface{}{"ssl": "true"},
		}
		dbName := "test"
		urlWithPort := "mongodb://user:password@localhost:27017/test?authSource=admin&directConnection=true&ssl=true"

		config := &config.ConfigProviderMock{
			GetResourceInfoFunc: func(resourceType, resourcePort, resourceName string) (*providers.ResourceInfo, error) {
				return resInfo, nil
			},
		}
		actual, err := createConnectionString(config, dbName)
		assert.NoError(t, err)
		assert.Equal(t, urlWithPort, actual)
	})

	t.Run("verify that mongodb connection string contains ssl false", func(t *testing.T) {
		resInfo := &providers.ResourceInfo{
			Host:        "localhost",
			Port:        "27017",
			Credentials: map[string]string{"username": "user", "password": "password"},
			Options:     map[string]interface{}{"ssl": "false"},
		}
		dbName := "test"
		urlWithPort := "mongodb://user:password@localhost:27017/test?authSource=admin&directConnection=true&ssl=false"

		config := &config.ConfigProviderMock{
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
		config := &config.ConfigProviderMock{
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
