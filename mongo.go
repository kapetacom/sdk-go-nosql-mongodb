// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT

package mongo

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/kapetacom/sdk-go-config/providers"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const RESOURCE_TYPE = "kapeta/resource-type-mongodb"
const RESOURCE_PORT = "mongodb"

type MongoDB struct {
	*mongo.Client
	dbName string
}

func (m *MongoDB) DB() *mongo.Database {
	return m.Database(m.dbName)
}

func NewMongoDB(config providers.ConfigProvider, resourceName string) (*MongoDB, error) {
	resInfo, err := config.GetResourceInfo(RESOURCE_TYPE, RESOURCE_PORT, resourceName)
	if err != nil {
		return nil, err
	}

	url, err := createConnectionString(resInfo, resourceName)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	log.Printf("Connecting to mongodb database: %s\n", resourceName)

	options := options.Client().
		ApplyURI(url).
		SetAppName(config.GetBlockReference()).
		// Enable automatic retrying of writes if they fail due to transient errors
		SetRetryWrites(true).
		// Enable automatic retrying of reads if they fail due to transient errors
		SetRetryReads(true)

	// Maximum number of connections allowed in the connection pool
	// Higher values allow more concurrent operations but consume more resources
	maxPoolSize := envGetInt("MONGO_MAX_POOL_SIZE", 50)
	options.SetMaxPoolSize(uint64(maxPoolSize))

	// Minimum number of connections to maintain in the connection pool
	// Helps reduce connection overhead by keeping a baseline of ready connections
	minPoolSize := envGetInt("MONGO_MIN_POOL_SIZE", 10)
	options.SetMinPoolSize(uint64(minPoolSize))

	// Maximum time a connection can remain idle in the pool before being removed
	// Helps clean up unused connections to free up resources
	maxConnIdleTime := envGetInt("MONGO_MAX_CONN_IDLE_TIME", 120)
	options.SetMaxConnIdleTime(time.Duration(maxConnIdleTime) * time.Second)

	// Maximum time to wait for a connection to be established with the server
	// Prevents hanging indefinitely when the server is unreachable
	connectTimeout := envGetInt("MONGO_CONNECT_TIMEOUT", 10)
	options.SetConnectTimeout(time.Duration(connectTimeout) * time.Second)

	// Maximum time to wait for server selection during operations
	// Useful when working with replica sets to prevent long waits for primary selection
	serverSelectionTimeout := envGetInt("MONGO_SERVER_SELECTION_TIMEOUT", 5)
	options.SetServerSelectionTimeout(time.Duration(serverSelectionTimeout) * time.Second)

	// Maximum time to wait for database operations to complete
	// Prevents operations from hanging indefinitely and ensures timely failures
	timeout := envGetInt("MONGO_TIMEOUT", 30)
	options.SetTimeout(time.Duration(timeout) * time.Second)

	// Write concern determines the level of acknowledgment requested from MongoDB for write operations
	// "majority" ensures writes are acknowledged by a majority of replica set members for strong consistency
	writeMajority := "majority"
	if os.Getenv("MONGO_WRITE_MAJORITY") != "" {
		writeMajority = envGetString("MONGO_WRITE_MAJORITY", writeMajority)
	}
	options.SetWriteConcern(&writeconcern.WriteConcern{
		W: writeMajority,
	})

	client, err := mongo.Connect(options)
	if err != nil {
		return nil, err
	}
	log.Println("Checking connection to mongodb database")
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("Connected successfully to mongodb database: %s\n", resourceName)

	return &MongoDB{client, getDBName(resInfo, resourceName)}, nil
}

func createConnectionString(resInfo *providers.ResourceInfo, resourceName string) (string, error) {
	protocol := getProtocol(resInfo)
	dbName := getDBName(resInfo, resourceName)

	url := ""
	if protocol == "mongodb+srv" {
		url = fmt.Sprintf("%s://%s:%s@%s/%s", protocol, resInfo.Credentials["username"], resInfo.Credentials["password"], resInfo.Host, dbName) + "?authSource=admin"
	} else {
		url = fmt.Sprintf("%s://%s:%s@%s:%s/%s", protocol, resInfo.Credentials["username"], resInfo.Credentials["password"], resInfo.Host, resInfo.Port, dbName) + "?authSource=admin&directConnection=true"
	}
	if resInfo.Options["ssl"] != nil {
		url += fmt.Sprintf("&ssl=%s", resInfo.Options["ssl"])
	}
	return url, nil
}

func getProtocol(resInfo *providers.ResourceInfo) string {
	if resInfo.Options["protocol"] != nil && resInfo.Options["protocol"] != "" {
		return fmt.Sprintf("%v", resInfo.Options["protocol"])
	}
	return "mongodb"
}

func getDBName(resInfo *providers.ResourceInfo, resourceName string) string {
	if resInfo.Options["dbName"] != nil && resInfo.Options["dbName"] != "" {
		return fmt.Sprintf("%v", resInfo.Options["dbName"])
	}
	return resourceName
}

func envGetInt(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}
	return i
}

func envGetString(key string, defaultValue string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	return val
}
