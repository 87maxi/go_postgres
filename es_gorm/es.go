package es_gorm

import (
	//Postgres Driver imported
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v8"
	esapi "github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

//var bi esutil.BulkIndexer

func SingleInsert(Jstruct *Pg, client *elastic.Client) {

	jsonData, err := json.Marshal(Jstruct)

	req := esapi.IndexRequest{
		Index:      "imdb",                              // Index name
		Body:       strings.NewReader(string(jsonData)), // Document body
		DocumentID: uuid.New().String(),                 // Document ID
		//Refresh:    "true",                              // Refresh

	}

	if err != nil {
		fmt.Println(err)
	}

	res, err := req.Do(context.Background(), client)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	log.Println(res)

}

func GetAllIndex() {

	client, err := GetESClient()

	if err != nil {
		panic("ERRORRRRRRRR")
	}

	ctx := context.Background()

	var query = `{
			"query": {
			  "match_all": {}
			}
		  }`

	var buf bytes.Buffer

	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())

	fmt.Println("read:", read)
	fmt.Println("read TYPE:", reflect.TypeOf(read))
	fmt.Println("JSON encoding:", json.NewEncoder(&buf).Encode(read))

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex("kibana_sample_data_ecommerce"),
		client.Search.WithBody(read),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
	)

	// Check for any errors returned by API call to Elasticsearch
	if err != nil {

		fmt.Printf("Elasticsearch Search() API ERROR: %s", err)
	}

	fmt.Println(res.String())

}

func CretaIndex() {
	var client, err = GetESClient()
	response, err := client.Indices.Create("imdb")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	if response.IsError() {
		log.Println(err)
		os.Exit(1)
	}
}

func IncertBulk(pg *Pg, bi esutil.BulkIndexer) {

	data, err := json.Marshal(&pg)

	if err != nil {
		log.Fatalf("Cannot encode article %s: %s", pg.Tconst, err)
	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Add an item to the BulkIndexer
	//
	err = bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "create",

			// DocumentID is the (optional) document ID
			DocumentID: uuid.New().String(),

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			/*OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				atomic.AddUint64(&countSuccessful, 1)
			},*/

			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("ERROR: %s", err)
				} else {
					log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
	if err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

}

func GetESClient() (*elastic.Client, error) {

	client, err := elastic.NewDefaultClient()

	fmt.Println("ES initialized...")

	return client, err

}

func clear(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

var client, _ = GetESClient()

func EsSync(pg *Pg) {

	//SingleInsert(&pg, client)

	//fmt.Println(&data)

	var bulk, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "imdb",          // The default index name
		Client:        client,          // The Elasticsearch client
		NumWorkers:    4,               // The number of worker goroutines
		FlushBytes:    int(10),         // The flush threshold in bytes
		FlushInterval: 1 * time.Second, // The periodic flush interval
	})

	if err != nil {
		fmt.Println("bulk errorr ")
	}
	IncertBulk(pg, bulk)

	clear(&pg.Names)
	clear(&pg.Akas)
	clear(&pg.Basics)

}
