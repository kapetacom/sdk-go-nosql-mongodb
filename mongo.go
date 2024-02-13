package mongo

import (
	"context"
	"fmt"
	"log"
	"time"

	sdkgoconfig "github.com/kapetacom/sdk-go-config"
	"github.com/kapetacom/sdk-go-config/providers"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const RESOURCE_TYPE = "kapeta/resource-type-mongodb"
const RESOURCE_PORT = "mongodb"

type MongoDB struct {
	resourceName string
	ready        bool
	mongo        *mongo.Client
}

func NewMongoDB(resourceName string) *MongoDB {
	db := &MongoDB{
		resourceName: resourceName,
	}

	go db.init()
	return db
}

func createMongoDBClient(config providers.ConfigProvider, resourceName string) (*mongo.Client, error) {
	url, err := createConnectionString(config, resourceName)
	if err != nil {
		return nil, err
	}
	url = url + "?authSource=admin&directConnection=true"

	ctx := context.Background()
	log.Printf("Connecting to mongodb database: %s\n", resourceName)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url).SetAppName("kapeta"))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("Connected successfully to mongodb database: %s\n", resourceName)

	return client, nil
}

func (db *MongoDB) init() {
	sdkgoconfig.CONFIG.OnReady(func(provider providers.ConfigProvider) {
		mongo, err := createMongoDBClient(provider, db.resourceName)
		if err != nil {
			panic(err)
		}

		db.mongo = mongo
		db.ready = true
	})
}

func (db *MongoDB) Client() (*mongo.Client, error) {
	for !db.ready {
		time.Sleep(time.Millisecond * 100)
	}

	if db.mongo == nil {
		return nil, fmt.Errorf("MongoDB not ready")
	}

	return db.mongo, nil
}

func createConnectionString(config providers.ConfigProvider, resourceName string) (string, error) {
	resInfo, err := config.GetResourceInfo(RESOURCE_TYPE, RESOURCE_PORT, resourceName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", resInfo.Credentials["username"], resInfo.Credentials["password"], resInfo.Host, resInfo.Port, resInfo.Options["dbName"]), nil
}
