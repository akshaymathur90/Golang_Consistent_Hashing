package main

import (
	"github.com/drone/routes"
	"log"
	"strconv"
	"encoding/json"
	"os"
	"net/http"
	"strings"
	"fmt"
)

type datastore struct{
	Key string
	Value string
}

var data []map[string]string
var portStart int64
func main(){

	portRange:= os.Args[1]
	portNums:=strings.Split(portRange,"-")
	//log.Println(portNums[0])
	//log.Println(portNums[1])
	portStart,_=strconv.ParseInt(portNums[0],10,64)
	portEnd,_:=strconv.ParseInt(portNums[1],10,64)
	
	mux := routes.New()
	mux.Get("/", GetAll)
	mux.Put("/:key/:value",PutKeyValue)
	mux.Get("/:key",GetWithKey)
	http.Handle("/", mux)
	
	for i:=portStart;i<=portEnd;i++{
		log.Println(i)
		log.Println("launch")
		data=append(data,map[string]string{})
		go launchServer(i)
	}
	
	
	var input string
    fmt.Scanln(&input)
    fmt.Println("done")
	
	
}



func launchServer(port int64){
	fmt.Println("Launching server "+strconv.FormatInt(port, 10))
	
	fmt.Println("Listening...on "+strconv.FormatInt(port, 10))
	http.ListenAndServe(":"+strconv.FormatInt(port, 10), nil)
}

func GetAll(w http.ResponseWriter, r *http.Request){
	log.Println("in Get All")
	name := r.Host
	log.Println(name)
	log.Println(portStart)
	incomPort,_:=strconv.ParseInt(strings.Split(name,":")[1],10,64)
	log.Println(incomPort-portStart)
	returnJson, _ := json.Marshal(data[incomPort-portStart])
	log.Println(data)
	w.Write([]byte(returnJson))

}
func PutKeyValue(w http.ResponseWriter, r *http.Request){
	log.Println("in Put")
	
	params := r.URL.Query()
	key := params.Get(":key")
	value := params.Get(":value")

	name := r.Host
	log.Println(name)
	log.Println(portStart)
	incomPort,_:=strconv.ParseInt(strings.Split(name,":")[1],10,64)
	log.Println(incomPort-portStart)
	data[incomPort-portStart][key]=value
	log.Println(data)
	w.WriteHeader(http.StatusNoContent)

}
func GetWithKey(w http.ResponseWriter, r *http.Request){
	log.Println("in Get with Key")
	
	params := r.URL.Query()
	key := params.Get(":key")
	
	name := r.Host
	log.Println(name)
	log.Println(portStart)
	incomPort,_:=strconv.ParseInt(strings.Split(name,":")[1],10,64)
	log.Println(incomPort-portStart)
	
	var newd datastore
	newd.Key=key
	newd.Value=data[incomPort-portStart][key]
	returnJson, _ := json.Marshal(newd)
	log.Println(data)
	w.Write([]byte(returnJson))

}