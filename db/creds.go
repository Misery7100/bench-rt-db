package db

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type DbCreds struct {
	Timescale  DbCredsUnit `yaml:"timescale"`
	ClickHouse DbCredsUnit `yaml:"clickhouse"`
	RethinkDB  DbCredsUnit `yaml:"rethinkdb"`
}

type DbCredsUnit struct {
	User     string `yaml:"user"`
	Dbname   string `yaml:"dbname"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
}

func ReadDbCreds(filename string) DbCreds {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("Error reading YAML file: %v", err)
	}

	var creds DbCreds
	err = yaml.Unmarshal(data, &creds)
	if err != nil {
		log.Printf("Error parsing YAML file: %v", err)
	}

	return creds
}
