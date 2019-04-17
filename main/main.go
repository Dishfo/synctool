package main

import (
	"flag"
	"log"
	"net/http"
)


//注册handshake handler 驱动fsystem
func init() {

}


func main(){
	flag.Parse()
	fill()
	err := vaildation()
	if err!=nil {
		log.Fatal(err)
	}
	initNode()
	initFs()
	route()
	log.Fatal(http.ListenAndServe(configs[TagHttpListen],nil))
}





















