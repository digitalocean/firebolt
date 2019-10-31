// +build integration

package elasticsearch

import (
	"context"
	"errors"

	"github.com/olivere/elastic/v7"
)

func CreateIndex(indexName string) error {
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://localhost:9200"))
	if err != nil {
		return err
	}

	mapping := `{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"msg":{
				"type":"text",
				"index": true
			}
		}
	}
}`

	createResult, err := client.CreateIndex(indexName).BodyString(mapping).Do(context.Background())
	if err != nil {
		return err
	}
	if !createResult.Acknowledged {
		return errors.New("create index not acknowledged")
	}
	return nil
}

// searchAllDocuments is a test utility for fetching all documents in the specified elasticsearch index.
func SearchAllDocuments(indexName string) (*elastic.SearchHits, error) {
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://localhost:9200"))
	if err != nil {
		return nil, err
	}

	elastic.NewMatchAllQuery()
	resp, err := client.Search(indexName).Query(elastic.NewMatchAllQuery()).TrackTotalHits(true).Do(context.Background())
	if err != nil {
		return nil, err
	}

	return resp.Hits, nil
}
