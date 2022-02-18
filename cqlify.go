package cqlify

import (
	"cqlify/cqlparser"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	tableFilePath, queryFilePath := setArgs()
	tableFile, queryFile := readFileArgs(tableFilePath, queryFilePath)
	_ = queryFile
	table, _ := cqlparser.ParseTable(tableFile)
	fmt.Sprintln(table)
}

func readFileArgs(tableFilePath, queryFilePath string) (tableFile, queryFile io.Reader) {
	raiseError := func(filepath string, err error) {
		if err != nil {
			log.Fatalf("Failed to read file '%s': %v\n\n", tableFile, err)
		}
	}

	tableFile, err := os.Open(tableFilePath)
	raiseError(tableFilePath, err)

	queryFile, err = os.Open(tableFilePath)
	raiseError(queryFilePath, err)

	return tableFile, queryFile
}

func setArgs() (string, string) {
	var tablePathFlag, queryPathFlag string
	flag.StringVar(&tablePathFlag, "table-definitions", "tables.cql", "path to file containing create table commands in cql")
	flag.StringVar(&tablePathFlag, "t", "tables.cql", "path to file containing create table commands in cql")
	flag.StringVar(&queryPathFlag, "queries", "queries.cql", "path to file containing intended queries in cql")
	flag.StringVar(&queryPathFlag, "q", "queries.cql", "path to file containing intended queries in cql")

	flag.Parse()
	return tablePathFlag, queryPathFlag
}
