package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/JamesStewy/go-mysqldump"
	"github.com/go-sql-driver/mysql"
	"os"
)

type Configuration struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	DbUser   string `json:"db_user"`
	Password string `json:"password"`
}

func main() {
	file, err := os.Open("conf.json")
	if err != nil {
		fmt.Println("Error loading configuration: ", err)
		return
	} else {
		fmt.Println("Configuration Loaded Successfully")
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err = decoder.Decode(&configuration)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	host := "localhost"
	if configuration.Host != "" {
		host = configuration.Host
	}
	port := "3306"
	if configuration.Port != "" {
		port = configuration.Port
	}
	config := mysql.NewConfig()
	config.User = ""
	if configuration.DbUser != "" {
		config.User = configuration.DbUser
	}
	config.Passwd = ""
	if configuration.Password != "" {
		config.Passwd = configuration.Password
	}
	config.Net = "tcp"
	config.Addr = host + ":" + port
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		fmt.Println("Error connection: ", err)
		return
	} else {
		fmt.Println("Connected Successfully ")
	}
	dbList, err := getDatabases(db)
	if err != nil {
		fmt.Println("Error opening database: ", err)
		return
	}
	db.Close()
	dbName := selectDatabase(dbList)
	config.DBName = dbName
	db, err = sql.Open("mysql", config.FormatDSN())
	if err != nil {
		fmt.Println("Error opening database: ", err)
		return
	}
	dumpDir := "dumps"
	dumpFilenameFormat := fmt.Sprintf("%s-%s-20060102T150405", host, config.DBName)

	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		err := os.Mkdir(dumpDir, 0777)
		if err != nil {
			fmt.Println("Error creating directory: ", err)
			return
		}
	}
	mysqlDump(db, dumpDir, dumpFilenameFormat)
	db.Close()
}

func getDatabases(db *sql.DB) ([]string, error) {
	var dbs string
	var databases []string
	dbFilter := []string{"performance_schema", "sys", "mysql", "information_schema"}
	row, err := db.Query("SHOW DATABASES")
	filter := true
	if err != nil {
		fmt.Println("Error opening database: ", err)
		return []string{}, err
	}
	for row.Next() {
		row.Scan(&dbs)

		if filter {
			if !contains(dbFilter, dbs) {
				databases = append(databases, dbs)
			}
		} else {
			databases = append(databases, dbs)
		}

	}
	defer row.Close()
	return databases, nil
}

func selectDatabase(databaseString []string) string {
a:
	fmt.Println("List of Database for Backup: ")
	for i, s := range databaseString {
		fmt.Printf("%s ----> %v\n", s, i+1)
	}
	fmt.Println("Select Database for backup : ")
	var dbIndex int
	_, err := fmt.Scan(&dbIndex)
	if err != nil {
		fmt.Println("Invalid selection")
		goto a
	}
	if dbIndex <= 0 || dbIndex > len(databaseString) {
		fmt.Println("Invalid selection")
		goto a
	}
	fmt.Println("selectedDB is : ", databaseString[dbIndex-1])
	return databaseString[dbIndex-1]
}

func mysqlDump(db *sql.DB, dumpDir string, dumpFilenameFormat string) {
	dumper, err := mysqldump.Register(db, dumpDir, dumpFilenameFormat)

	if err != nil {
		fmt.Println("Error registering database:", err)
		return
	}

	resultFilename, err := dumper.Dump()

	if err != nil {
		fmt.Println("Error dumping:", err)
		return
	}
	fmt.Printf("File is saved to %s", resultFilename)

	dumper.Close()
}

func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}
