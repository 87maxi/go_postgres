package es_gorm

import (
	"context"
	"database/sql"
	"fmt"

	"log"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ctx context.Context
	db  *sql.DB
)

func Mysql_conect() {
	db, err := sql.Open("mysql", "vm:qwerty@/vm")
	if err != nil {
		panic(err)
	}
	// See "Important settings" section.

	if err = db.Ping(); err != nil {

		log.Fatalf("Cannot ping database because %s", err)

	}

	rows, err := db.QueryContext(context.TODO(), `SELECT title, languge from akas  where title like '%ü%'  ;`)
	if err != nil {
		log.Fatalf("Unable to retrieve customers because %s", err)
	}

	for rows.Next() {
		var c Akas

		err = rows.Scan(&c.Title, &c.Language)

		if err != nil {
			log.Fatalf("Unable to scan row for customer because %s", err)
		}
		fmt.Println(c)
	}

	fmt.Println("asdfasdf")

}
