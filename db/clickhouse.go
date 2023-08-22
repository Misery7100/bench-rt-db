package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseDB struct {
	conn driver.Conn
}

func NewClickHouseDBv2(creds DbCreds) (*ClickHouseDB, error) {
	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:9440", creds.ClickHouse.Host)},
		Auth: clickhouse.Auth{
			Database: creds.ClickHouse.Dbname,
			Username: creds.ClickHouse.User,
			Password: creds.ClickHouse.Password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "an-example-go-client", Version: "0.1"},
			},
		},

		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	if err != nil {
		return nil, err
	}

	// if err := conn.Ping(ctx); err != nil {
	// 	if exception, ok := err.(*clickhouse.Exception); ok {
	// 		fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
	// 	}
	// 	return nil, err
	// }

	CreateTable(conn, ctx)

	clickhouseDB := &ClickHouseDB{conn: conn}

	return clickhouseDB, nil
}

func CreateTable(conn driver.Conn, ctx context.Context) {
	query := `
		CREATE TABLE IF NOT EXISTS stocks (
			timestamp Float64,
			symbol String,
			price Float64
		) ENGINE MergeTree
		ORDER BY timestamp;
	`
	err := conn.Exec(ctx, query)

	if err != nil {
		log.Printf("Failed to create table: %v", err)
	}
}

func (db *ClickHouseDB) Write(data map[string]interface{}) {
	ctx := context.Background()
	err := db.conn.Exec(ctx, "INSERT INTO stocks (timestamp, symbol, price) VALUES (?, ?, ?)",
		data["timestamp"], data["symbol"], data["price"])
	if err != nil {
		log.Println("Error writing to ClickHouseDB:", err)
	}
}

func (db *ClickHouseDB) Read(timestamp int64) {
	ctx := context.Background()
	rows, err := db.conn.Query(ctx, "SELECT * FROM stocks WHERE timestamp = ?", timestamp)
	if err != nil {
		log.Println("Error reading from ClickHouseDB:", err)
	}
	defer rows.Close()
}

// func NewClickHouseDB(creds DbCreds) *ClickHouseDB {
// 	connStr := fmt.Sprintf(
// 		"tcp://%s:%s?username=%s&password=%s&database=%s",
// 		creds.ClickHouse.Host, creds.ClickHouse.Port, creds.ClickHouse.User,
// 		creds.ClickHouse.Password, creds.ClickHouse.Dbname)

// 	db, err := sql.Open("clickhouse", connStr)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	clickhouseDB := &ClickHouseDB{conn: db}
// 	clickhouseDB.createTable() // Create table if it doesn't exist

// 	return clickhouseDB
// }

// func (db *ClickHouseDB) createTable() {
// 	query := `
// 		CREATE TABLE IF NOT EXISTS stocks (
// 			timestamp Float64,
// 			symbol String,
// 			price Float64
// 		) ENGINE MergeTree
// 		ORDER BY timestamp;
// 	`
// 	_, err := db.conn.Exec(query)
// 	if err != nil {
// 		log.Printf("Failed to create table: %v", err)
// 	}
// }

// func (db *ClickHouseDB) Write(data map[string]interface{}) {
// 	_, err := db.conn.Exec("INSERT INTO stocks (timestamp, symbol, price) VALUES ($1, $2, $3)",
// 		data["timestamp"], data["symbol"], data["price"])
// 	if err != nil {
// 		log.Println("Error writing to ClickHouseDB:", err)
// 	}
// }

// func (db *ClickHouseDB) Read(timestamp int64) {
// 	rows, err := db.conn.Query("SELECT * FROM stocks WHERE timestamp = $1", timestamp)
// 	if err != nil {
// 		log.Println("Error reading from ClickHouseDB:", err)
// 	}
// 	defer rows.Close()
// }
