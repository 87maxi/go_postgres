package es_gorm

import (
	"fmt"
	"log"

	"github.com/jinzhu/gorm"
)

var err error

type Pg struct {
	Tconst   string
	Category string
	Nconst   string
	Akas     []Akas
	Basics   []Basics
	Names    []Names
}

type Akas struct {
	Title string
	//tconst  string `json:tconst`
	Titleid         string `json:titleid`
	Region          string `json:region`
	Language        string `json:language`
	Ordering        string `json:ordering`
	Types           string `json:types`
	Attributes      string `json:attributes`
	Isoriginaltitle string `json:isoriginaltitle`
}

type Basics struct {
	Primarytitle  string `json:primarytitle`
	Originaltitle string `json:originaltitle`
	Tconst        string `json:tconst`
	Genres        string `json:geres`
	Titletype     string `json:titletype`
	//Aka          []Akas
}

type Names struct {
	Primaryname       string
	Primaryprofession string
	Nconst            string
	Deathyear         string
	Knownfortitles    string
}

func ConnectDB() *gorm.DB {
	var (
		host     = "localhost"
		user     = "vm"
		port     = 5432
		password = "qwerty"
		name     = "vm"
	)
	//Connect to DB
	var DB *gorm.DB

	var DSN = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, name)
	DB, err = gorm.Open("postgres", DSN)

	if err != nil {
		log.Fatalf("Error in connect the DB %v", err)
		return nil
	}
	if err := DB.DB().Ping(); err != nil {
		log.Fatalln("Error in make ping the DB " + err.Error())
		return nil
	}
	if DB.Error != nil {
		log.Fatalln("Any Error in connect the DB " + err.Error())
		return nil
	}
	log.Println("DB connected")
	return DB

}

var DB = ConnectDB()

func BasicsF() {
	var pg Pg
	var b Basics
	//var pg Pg
	ch := make(chan Basics)
	var bas, err = DB.Table("basics").Select("*").Rows()
	defer bas.Close()
	if err != nil {
		fmt.Printf("principals error")
	}
	//wg.Add(5)
	go func() {

		for bas.Next() {

			DB.ScanRows(bas, &b)

			ch <- b

		}
		close(ch)
	}()

	for e := range ch {

		akasf(e, &pg)
		namesf(e, &pg)

	}

}

func akasf(x Basics, pg *Pg) {
	var a Akas

	ch := make(chan Akas)
	akas, err := DB.Model(&Akas{}).Where("titleid = ?", x.Tconst).Rows()

	if err != nil {
		fmt.Printf("principals error")
	}
	defer akas.Close()
	go func() {

		for akas.Next() {

			DB.ScanRows(akas, &a)

			ch <- a

		}
		close(ch)
	}()

	for e := range ch {
		pg.Akas = append(pg.Akas, e)

	}
	pg.Basics = append(pg.Basics, x)

}

func namesf(x Basics, pg *Pg) {
	var a Names

	query := fmt.Sprintf("%s%s%s", "%", x.Tconst, "%")
	fmt.Println(query)

	ch := make(chan Names)
	names, err := DB.Model(&Names{}).Debug().Where("knownfortitles LIKE ?", query).Rows()
	//names, err := DB.Model(&Names{}).Debug().Where("to_tsvector(knownfortitles) @@ to_tsquery( ? )", x.Tconst).Rows()
	if err != nil {
		fmt.Printf("principals error")
	}
	defer names.Close()
	go func() {

		for names.Next() {

			DB.ScanRows(names, &a)

			ch <- a
		}
		close(ch)
	}()

	for e := range ch {
		pg.Names = append(pg.Names, e)

	}
	fmt.Println(pg)
	EsSync(pg)

}
