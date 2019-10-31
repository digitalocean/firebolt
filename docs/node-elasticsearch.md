# Elasticsearch

 ||||
 |--------------|:--------:|--------|
 | **Accepts:** | `elasticsearch.IndexRequest` | Index requests specify the document to index and the target index name |
 | **Returns:** |  `*firebolt.AsyncEvent`      | For successfully indexed events, returns the original event |


The `elasticsearch` node indexes documents to an Elasticsearch cluster.   Its parent node must return 
`elasticsearch.IndexRequest` structs containing - at a minimum - the document and target index name.

The IndexRequest type contains four fields:

Field                   | Required | Default | Description              
------------------------|:--------:|---------|--------------
Index                   |  *       |         | Destination index name in Elasticsearch.
MappingType             |          |         | In ES 7.x+, omit MappingType and ES will default to `_doc`.  In ES 6.x-, specify your mapping type. 
DocID                   |          |         | Provide a document ID to index this document under, or leave nil and ES will assign an ID.
Doc                     |  *       |         | The document to index.  This should be a struct that is marshallable to JSON.

Internally, `elasticsearch` uses the `BulkService` in the `olivere/elastic` client.

## Configuration

Param                     | Required | Default | Description              
--------------------------|:--------:|---------|--------------
elastic-addr              |  *       |         | Comma-separated list of Elasticsearch client nodes.
batch-size                |          | 100     | Wait until this many documents are collected and send them as a single batch.
batch-max-wait-ms         |          | 1000    | Max time, in ms, to wait for `batch-size` documents to be ready before sending a smaller batch.
bulk-index-timeout-ms     |          | 5000    | Timeout passed to Elasticsearch along with the bulk index request.
reconnect-batch-count     |          | 10000   | Reestablish connections to ES after this many batches.  Useful to ensure that load remains distributed among ES client nodes as they are created or fail.
bulk-index-max-retries    |          | 3       | Number of times to retry indexing errors.   Mapping errors / field type conflicts are not retryable.
bulk-index-timeout-seconds|          | 20      | Forcibly cancel individual bulk indexing operations after this time.
index-workers             |          | 1       | Number of goroutine workers to use to process batch indexing operations against Elasticsearch.
