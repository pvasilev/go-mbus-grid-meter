package sql_file_importer

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	EnvKeyPgHost   = "GO_IMP_PG_HOST"
	EnvKeyPgPort   = "GO_IMP_PG_PORT"
	EnvKeyPgUser   = "GO_IMP_PG_USER"
	EnvKeyPgPass   = "GO_IMP_PG_PASS"
	EnvKeyPgDbname = "GO_IMP_PG_DBNAME"

	CliFlagInputFile = "infile"
	CliFlagConfFile  = "conf"
	CliFlagPgHost    = "pghost"
	CliFlagPgPort    = "pgport"
	CliFlagPgUser    = "pguser"
	CliFlagPgPass    = "pgpass"
	CliFlagPgDbname  = "pgdbname"
)

type PgSqlConf struct {
	Host     string `json:"pghost"`
	Port     int    `json:"pgport"`
	UserName string `json:"pguser"`
	Password string `json:"pgpass"`
	Dbname   string `json:"pgdbname"`
}

type CliFlags struct {
	InputFile     *string
	ConfFile      *string
	Host          *string
	Port          *int
	UserName      *string
	Password      *string
	Dbname        *string
	IsInitialized bool
}

var cliFlags = new(CliFlags)

func initializeFlags() {

	cliFlags.InputFile = flag.String(CliFlagInputFile,"", "Input SQL File")
	cliFlags.ConfFile = flag.String(CliFlagConfFile,"", "Configuration file")
	cliFlags.Host = flag.String(CliFlagPgHost,"localhost", "Postgres Server Hostname")
	cliFlags.Port = flag.Int(CliFlagPgPort, 5432, "Postgres Server Port")
	cliFlags.UserName = flag.String(CliFlagPgUser,"", "Postgres Server Login Username")
	cliFlags.Password = flag.String(CliFlagPgPass,"", "Postgres Server Login Password")
	cliFlags.Dbname = flag.String(CliFlagPgDbname,"", "Postgres Server database name")
	cliFlags.IsInitialized = true
}

func parseConfFromJsonFile(fileName string) (*PgSqlConf, error) {
	jsonFile, err := os.Open(fileName)
	if err != nil {
		log.Printf("Failed to open JSON file %q", fileName)
		return nil, err
	}
	defer jsonFile.Close()

	fileData, err := ioutil.ReadAll(jsonFile)

	if err != nil {
		log.Printf("Failed to load data from file %q", fileName)
		return nil, err
	}

	var conf PgSqlConf
	err = json.Unmarshal(fileData, &conf)
	if err != nil {
		log.Printf("Failed to unmarshal file contents from %q to struct", fileName)
		return nil, err
	}

	return &conf, nil
}

func parseConfFromPropertiesFile(fileName string) (*PgSqlConf, error) {
	confFile, err := os.Open(fileName)
	if err != nil {
		log.Printf("Failed to open properties file %q", fileName)
		return nil, err
	}
	defer confFile.Close()

	var properties = make(map[string]string)

	scanner := bufio.NewScanner(confFile)
	for scanner.Scan() {
		line := scanner.Text()
		if equalsIdx := strings.Index(line, "="); equalsIdx > 0 {
			if keyName := strings.TrimSpace(line[:equalsIdx]); len(keyName) > 0 {
				value := ""
				if len(line) > equalsIdx {
					value = strings.TrimSpace(line[equalsIdx+1:])
				}
				properties[keyName] = value
			}
		}
	}

	conf := new(PgSqlConf)

	conf.Host = properties[CliFlagPgHost]
	port, err := strconv.Atoi(properties[CliFlagPgPort])
	if err != nil {
		return nil, err
	}
	conf.Port = port
	conf.UserName = properties[CliFlagPgUser]
	conf.Password = properties[CliFlagPgPass]
	conf.Dbname = properties[CliFlagPgDbname]

	return conf, nil
}

func extractConfFromEnvironment() (*PgSqlConf, error) {
	conf := new(PgSqlConf)

	host, isPresent := os.LookupEnv(EnvKeyPgHost)
	if isPresent {
		conf.Host = host
	}
	sPort, isPresent := os.LookupEnv(EnvKeyPgPort)
	if isPresent {
		port, err := strconv.Atoi(sPort)
		if err != nil {
			log.Printf("Failed to convert %q to port", sPort)
			return nil, err
		}
		conf.Port = port
	}
	userName, isPresent := os.LookupEnv(EnvKeyPgUser)
	if isPresent {
		conf.UserName = userName
	}
	password, isPresent := os.LookupEnv(EnvKeyPgPass)
	if isPresent {
		conf.Password = password
	}
	dbName, isPresent := os.LookupEnv(EnvKeyPgDbname)
	if isPresent {
		conf.Dbname = dbName
	}

	return conf, nil
}

func extractConfFromCommandLineParams(cliFlagsPtrs CliFlags) (*PgSqlConf, error) {

	if ! cliFlagsPtrs.IsInitialized {
		return nil, errors.New("command line flags struct not initialized")
	}

	conf := new(PgSqlConf)
	conf.Host = *cliFlagsPtrs.Host
	conf.Port = *cliFlagsPtrs.Port
	conf.UserName = *cliFlagsPtrs.UserName
	conf.Password = *cliFlagsPtrs.Password
	conf.Dbname = *cliFlagsPtrs.Dbname

	return conf, nil
}

func mergeConfigurations(conf *PgSqlConf, other PgSqlConf) {

	if len(other.Host) > 0 { conf.Host = other.Host }
	if other.Port > 0 { conf.Port = other.Port }
	if len(other.UserName) > 0 { conf.UserName = other.UserName }
	if len(other.Password) > 0 { conf.Password = other.Password }
	if len(other.Dbname) > 0 { conf.Dbname = other.Dbname }

}

// ProcessConfiguration tries to extract the configuration for the tool from command line, environment of specified file
func ProcessConfiguration() (*CliFlags, *PgSqlConf, error) {
	initializeFlags()
	flag.Parse()
	conf := new(PgSqlConf)

	// First try to get from environment
	confEnv, err := extractConfFromEnvironment()
	if err != nil {
		log.Printf("Failed to extract configuration from environment")
		return nil, nil, err
	}
	mergeConfigurations(conf, *confEnv)

	if (len(*cliFlags.ConfFile)) > 0 {
		fileName := *cliFlags.ConfFile
		if filepath.Ext(fileName) == ".json" {
			// Try to get configuration from JSON file
			confJson, err := parseConfFromJsonFile(fileName)
			if err != nil {
				log.Printf("Failed to extract configuration from JSON file "+fileName)
				return nil, nil, err
			}
			mergeConfigurations(conf, *confJson)
		} else if (filepath.Ext(fileName) == ".ini") || (filepath.Ext(fileName) == ".properties") || (filepath.Ext(fileName) == ".props") {
			// Try to get configuration from Properties/INI type file
			confProps, err := parseConfFromPropertiesFile(fileName)
			if err != nil {
				log.Printf("Failed to extract configuration from Properties file "+fileName)
				return nil, nil, err
			}
			mergeConfigurations(conf, *confProps)
		} else {
			return nil, nil, errors.New("Unrecognized configuration file "+fileName)
		}
	}

	// And lastly try to extract from command line, the parameters set here take precedence over all others
	confCli, err := extractConfFromCommandLineParams(*cliFlags)
	if err != nil {
		log.Printf("Failed to extract configuration from environment")
		return nil, nil, err
	}
	mergeConfigurations(conf, *confCli)

	return cliFlags, conf, nil
}