package db

import (
	"fmt"
	"log"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type RethinkDB struct {
	conn *r.Session
}

func NewRethinkDB(creds DbCreds) *RethinkDB {
	session, err := r.Connect(r.ConnectOpts{
		Address: fmt.Sprintf("%s:%s", creds.RethinkDB.Host, creds.RethinkDB.Port), // endpoint without http
	})
	if err != nil {
		log.Fatalln(err)
	}

	rethinkDB := &RethinkDB{conn: session}
	rethinkDB.createTable() // Create table if it doesn't exist

	return rethinkDB
}

func (db *RethinkDB) createTable() {
	r.DB("test").TableCreate("stocks").Run(db.conn)
}

func (db *RethinkDB) Write(data map[string]interface{}) {
	r.DB("test").Table("stocks").Insert(data).Run(db.conn)
}

func (db *RethinkDB) Read(timestamp int64) {
	r.DB("test").Table("stocks").Get(timestamp).Run(db.conn)
}
