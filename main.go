package main

import (
	db "davait/dbbench/db"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
	// Import other necessary packages
)

func generateMockData(numRecords int) []map[string]interface{} {
	stockSymbols := []string{"AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"}
	data := make([]map[string]interface{}, numRecords)

	for i := 0; i < numRecords; i++ {
		stock := stockSymbols[rand.Intn(len(stockSymbols))]
		price := 100.0 + rand.Float64()*1400.0
		timestamp := time.Now().Unix()

		data[i] = map[string]interface{}{
			"symbol":    stock,
			"price":     price,
			"timestamp": timestamp,
		}
	}

	return data
}

func writeAndRead(database interface{}, data map[string]interface{}) {
	switch db := database.(type) {
	case *db.TimescaleDB:
		db.Write(data)
		db.Read(data["timestamp"].(int64))
	case *db.ClickHouseDB:
		db.Write(data)
		db.Read(data["timestamp"].(int64))
	case *db.RethinkDB:
		db.Write(data)
		db.Read(data["timestamp"].(int64))
	default:
		log.Println("Unsupported database type")
	}

}

func concurrentWriteReadTest(database interface{}, data []map[string]interface{}) {

	startTime := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(record map[string]interface{}) {
			defer wg.Done()
			writeAndRead(database, record)
		}(data[i])
	}

	wg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	fmt.Printf("Concurrent write-read test completed in %v.\n", elapsedTime)
}

func main() {
	mockData := generateMockData(500)

	if len(os.Args) < 2 {
		log.Printf("Usage: %s <path_to_creds.yaml>", os.Args[0])
	}
	credsFile := os.Args[1]
	creds := db.ReadDbCreds(credsFile)

	fmt.Println("Starting TimescaleDB test...")
	timescaleDB := db.NewTimescaleDB(creds)
	concurrentWriteReadTest(timescaleDB, mockData)
	fmt.Printf("TimescaleDB test completed.\n")

	fmt.Println("Starting ClickHouseDB test...")
	clickhouseDB, _ := db.NewClickHouseDBv2(creds)
	concurrentWriteReadTest(clickhouseDB, mockData)
	fmt.Printf("ClickHouseDB test completed.\n")

	fmt.Println("Starting RethinkDB test...")
	rethinkDB := db.NewRethinkDB(creds)
	concurrentWriteReadTest(rethinkDB, mockData)
	fmt.Printf("RethinkDB test completed.\n")

}
