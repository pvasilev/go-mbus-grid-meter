package main

import (
	"log"
	"os"
	"plamenv.com/mbusgridmeter/v1/sql_file_importer"
	"strconv"
)

func main() {
	if len(os.Args) != 8 {
		log.Fatalf("Invalid number of parameters. Expected 7, found %d", len(os.Args))
		return
	}
	fileName := os.Args[1]
	pgHost := os.Args[2]
	pgPort, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Failed to convert %q to Postgresql port", os.Args[3])
		return
	}
	pgUser := os.Args[4]
	pgPassword := os.Args[5]
	pgSchema := os.Args[6]
	pgTableName := os.Args[7]

	sqlFile, err := sql_file_importer.ReadSqlFile(fileName)
	if err != nil {
		log.Fatalf("Failed to read file %q", fileName)
		return
	}

	log.Printf("From %q loaded %d SQL statements", fileName, len(sqlFile.Lines))
	importedLines, err := sql_file_importer.ImportSqlFile(sqlFile, pgHost, pgPort, pgUser, pgPassword, pgSchema, pgTableName)
	if err != nil {
		log.Fatalf("Failed to import %q into %q/%q", fileName, pgSchema, pgTableName)
		return
	}
	log.Printf("Imported %d lines from %q into %q/%q", importedLines, fileName, pgSchema, pgTableName)
}
