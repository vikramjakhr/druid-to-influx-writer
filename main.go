package main

import (
	"log"
	"os"
	"os/signal"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"flag"
	"fmt"
)

func main() {
	port := flag.String("port", "7373", "")
	flag.Parse()
	log.Printf("Starting druid to influx writer on port: %s", *port)
	handleOSSignals()
	http.HandleFunc("/api/v1/feed/alert", Handler)
	http.HandleFunc("/api/v1/feed/metric", Handler)
	http.ListenAndServe(fmt.Sprintf(":%s", *port), nil)
}

func handleOSSignals() {
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		log.Println("OS Interrupt. Shutting down druid to influx writer.")
		os.Exit(0)
	}()
}

func Handler(rw http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		handlePOST(rw, req)
	} else {
		rw.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handlePOST(rw http.ResponseWriter, req *http.Request) {
	read, err := ioutil.ReadAll(req.Body)

	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	defer req.Body.Close()

	switch req.URL.Path {
	case "/api/v1/feed/alert":
		var data []map[string]interface{}

		err = json.Unmarshal(read, &data)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			rw.Write([]byte(`{"status":"error"}`))
			return
		}

		b, _ := json.MarshalIndent(data, "", "  ")
		log.Printf(string(b))
		break
	case "/api/v1/feed/metric":
		var data map[string]interface{}

		err = json.Unmarshal(read, &data)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			rw.Write([]byte(`{"status":"error"}`))
			return
		}

		b, _ := json.MarshalIndent(data, "", "  ")
		log.Printf(string(b))
		break
	}

	rw.WriteHeader(http.StatusOK)
	rw.Write([]byte(`{"status":"ok"}`))
}
