package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type TimescaleDB struct {
	conn *sql.DB
}

func NewTimescaleDB(creds DbCreds) *TimescaleDB {
	connStr := fmt.Sprintf(
		"user=%s dbname=%s password=%s host=%s port=%s sslmode=disable",
		creds.Timescale.User, creds.Timescale.Dbname, creds.Timescale.Password,
		creds.Timescale.Host, creds.Timescale.Port)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	timescaleDB := &TimescaleDB{conn: db}
	timescaleDB.createTable() // Create table if it doesn't exist

	return timescaleDB
}

func (db *TimescaleDB) createTable() {
	query := `
		CREATE TABLE IF NOT EXISTS stocks (
			timestamp NUMERIC NOT NULL,
			symbol TEXT NOT NULL,
			price FLOAT NOT NULL
		);
	`
	_, err := db.conn.Exec(query)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
	}
}

func (db *TimescaleDB) Write(data map[string]interface{}) {
	_, err := db.conn.Exec("INSERT INTO stocks (timestamp, symbol, price) VALUES ($1, $2, $3)",
		data["timestamp"], data["symbol"], data["price"])
	if err != nil {
		log.Println("Error writing to TimescaleDB:", err)
	}
}

func (db *TimescaleDB) Read(timestamp int64) {
	rows, err := db.conn.Query("SELECT * FROM stocks WHERE timestamp = $1", timestamp)
	if err != nil {
		log.Println("Error reading from TimescaleDB:", err)
	}
	defer rows.Close()
}
