package main

import (
	"log"
	"plamenv.com/mbusgridmeter/v1/sql_file_importer"
)

func main() {
	fullConfiguration, pgConfiguration, err := sql_file_importer.ProcessConfiguration()

	if err != nil {
		log.Fatalf("Could not create tool configuration due to: %q", err)

	}

	sqlFile, err := sql_file_importer.ReadSqlFile(*fullConfiguration.InputFile)
	if err != nil {
		log.Fatalf("Failed to read file %q", fullConfiguration.InputFile)
		return
	}

	log.Printf("From %q loaded %d SQL statements", fullConfiguration.InputFile, len(sqlFile.Lines))
	importedLines, err := sql_file_importer.ImportSqlFile(sqlFile, pgConfiguration)
	if err != nil {
		log.Fatalf("Failed to import file %q", fullConfiguration.InputFile)
		return
	}
	log.Printf("Imported %d lines from %q", importedLines, fullConfiguration.InputFile)
}
