package sql_file_importer

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

type SQLFile struct {
	FileName string
	Lines []string
}

func ReadSqlFile(fileName string) (*SQLFile, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result *SQLFile = new(SQLFile)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		result.Lines = append(result.Lines, scanner.Text())
	}
	return result, scanner.Err()
}

func ImportSqlFile(fileData *SQLFile, pgConf *PgSqlConf) (int, error) {
	return ImportSqlFileEx(fileData, pgConf.Host, pgConf.Port, pgConf.UserName, pgConf.Password, pgConf.Dbname)
}

func ImportSqlFileEx(fileData *SQLFile, host string, port int, user string, password string, dbname string) (int, error) {

	dsn := fmt.Sprintf("Host=%s Port=%d user=%s "+
		"Password=%s Dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", dsn)

	if err != nil {
		log.Fatalf("Failed to open connection to the postgresql server on %q:%d as %q", host, port, user)
		return 0, err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping Postgresql Dbname")
		return 0, err
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to open transaction to the Dbname")
		return 0, err
	}

	for i, s := range fileData.Lines {
		_, err := tx.Exec(s)
		if err != nil {
			msg := fmt.Sprintf("Failed to execute SQL: %q at file:'%q' line %d", s, fileData.FileName, i)
			err = tx.Rollback()
			if nil != err {
				msg += fmt.Sprintf("Additionally failed to rollback transaction to Dbname")
				return 0, err
			}
			log.Fatal(msg)
			return 0, err
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transaction to the Dbname")
		return 0, nil
	}

	return len(fileData.Lines), nil
}