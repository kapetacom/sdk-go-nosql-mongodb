// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT

package mongo

import (
	"context"
	"fmt"
	"log"

	"github.com/kapetacom/sdk-go-config/providers"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const RESOURCE_TYPE = "kapeta/resource-type-mongodb"
const RESOURCE_PORT = "mongodb"

type MongoDB struct {
	*mongo.Client
}

func NewMongoDB(config providers.ConfigProvider, resourceName string) (*MongoDB, error) {
	url, err := createConnectionString(config, resourceName)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	log.Printf("Connecting to mongodb database: %s\n", resourceName)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url).SetAppName(config.GetBlockReference()))
	if err != nil {
		return nil, err
	}

	log.Println("Checking connection to mongodb database")
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("Connected successfully to mongodb database: %s\n", resourceName)

	return &MongoDB{client}, nil
}

func createConnectionString(config providers.ConfigProvider, resourceName string) (string, error) {
	resInfo, err := config.GetResourceInfo(RESOURCE_TYPE, RESOURCE_PORT, resourceName)
	if err != nil {
		return "", err
	}
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
