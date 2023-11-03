package main

import (
	"fmt"
	"go_postgres/es_gorm"
	"log"
)

var err error

func main() {

	log.Println("Start Project")

	if err != nil {
		fmt.Printf("errrorr ")

	}

	es_gorm.BasicsF()
	es_gorm.DB.Close()

}
