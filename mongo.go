// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT

package mongo

import (
	"context"
	"fmt"
	"log"

	"github.com/kapetacom/sdk-go-config/providers"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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

	client, err := mongo.Connect(options.Client().ApplyURI(url).SetAppName(config.GetBlockReference()).SetRetryWrites(true).SetRetryReads(true))
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
