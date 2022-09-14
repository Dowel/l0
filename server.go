package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"log"
	"net/http"
)

// структура json для одной записи
type book struct {
	Id     string `json:"id"`
	Title  string `json:"title"`
	Author string `json:"author"`
	Price  string `json:"price"`
}

// создание переменной для хранения пула соединений
var db *sql.DB

// создание переменной для кеша
var cache map[string]book

func main() {
	// создание переменной для хранения ошибок
	var err error
	// открытие соединения с pg базой
	db, err := sql.Open("postgres", "user=postgres password=1 dbname=books sslmode=disable")

	// проверка на наличие ошибок при соединении с базой
	if err != nil {
		log.Fatal(err)
	}
	// закрытие соединения, очистка ресурсов
	defer db.Close()

	// проверка соединения, отдельно от выполняемых запросов
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	// заполнение кеша
	cache = make(map[string]book)
	get_cache(db)

	// подключение к nats-streaming
	sc, err := stan.Connect("test-cluster", "subscribe-id")
	if err != nil {
		log.Fatalln(err)
	}
	defer sc.Close()

	// подписка на канал book
	sub, err := sc.Subscribe("book", func(m *stan.Msg) {

		var data book

		// распределение считанных данных в соответствии с полями структуры book
		err = json.Unmarshal(m.Data, &data)
		if err != nil {
			log.Printf("New book not added: %s", err.Error())
			return
		}

		fmt.Println(data)
		if data.Id == "" {
			log.Printf("New order not added: empty Id")
			return
		}
		// вставляем данные по новой книге в таблицу бд
		_, err = db.Exec("INSERT INTO books VALUES($1, $2, $3, $4)", data.Id, data.Title, data.Author, data.Price)
		if err != nil {
			log.Printf("DB execution error, %s", err.Error())
			return
		}
		// добавление данных в map cache по ключу data id
		cache[data.Id] = data
		log.Printf("book added!")
	})
	defer sub.Close()

	// определение маршрутов с помощью gorilla/mux и сопоставление маршрута с определенными обработчиками
	router := mux.NewRouter()
	router.HandleFunc("/", home)
	router.HandleFunc("/books", show_all_books)
	router.HandleFunc("/books/{id}", get_book)
	err = http.ListenAndServe(":4000", router)

	if err != nil {
		log.Fatal(err)
	}
}

func get_cache(db *sql.DB) {
	// получение всех данных из таблицы books
	rows, err := db.Query("select * from books")
	if err != nil {
		log.Fatalln(err)
	}

	defer rows.Close()

	// перебор всех данных полученных из таблицы books
	for rows.Next() {
		bk := book{}
		err = rows.Scan(&bk.Id, &bk.Title, &bk.Author, &bk.Price)
		if err != nil {
			log.Fatal(err)
		}
		cache[bk.Id] = bk
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// функции-обработчики URL запросов

func home(w http.ResponseWriter, r *http.Request) {

	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	fmt.Fprintf(w, "URA YA ZAPUSTILSYA!!!!")
}

func show_all_books(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(405), 405)
		return
	}

	for _, value := range cache {
		jn, err := json.Marshal(value)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(w, "%s\n", jn)
	}
}

func get_book(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	for key, value := range cache {
		if key == params["id"] {
			js, err := json.Marshal(value)
			if err != nil {
				log.Println(err.Error())
				http.Error(w, "Internal Server Error", 500)
			}
			fmt.Fprintf(w, "%s", js)
			return
		}
	}
	fmt.Fprintf(w, "No book with this id!")
}
