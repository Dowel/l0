package main

import (
	"github.com/nats-io/stan.go"
	"io/ioutil"
	"log"
)

func main() {

	// создание пути к файлу для чтения
	file := []string{"model.json"}

	// подключение к nats-streaming
	sc, err := stan.Connect("test-cluster", "publisher-id")

	if err != nil {
		log.Fatalln(err)
	}

	defer sc.Close()

	for _, file := range file {

		// считывание содержимого файла
		fContent, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalln(err)
		}
		// передача данных считанных из файла
		sc.Publish("book", fContent)
	}
}
