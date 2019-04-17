package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
)

/**
提供配置文件解析
或者读取命令行参数
 */

const (
	TagDiscoveryServer = "DiscoverServer"
	TagHttpListen      = "HttpListen"
	TagNodeListen      = "NodeListen"
)

var (
	configFile      = ""
	discoveryServer = ""
	httpListen      = ""
	nodeListen      = ""

	configs = make(map[string]string)
)

func init() {
	flag.StringVar(&configFile, "cfile", "", ""+
		"config file location")

	flag.StringVar(&discoveryServer, "dsever", "", ""+
		"bootstrap server addr for discovery other node ")

	flag.StringVar(&httpListen, "weblisten", "", ""+
		"web server listen addr")

	flag.StringVar(&nodeListen, "listen", "", ""+
		"a libp2p node listen for communication")
}

//验证需要的参数是否完整
func vaildation() error {

	return nil
}

//将读取的参数填充填充到map中
func fill() {
	err := readConfigFile()
	if err != nil {
		log.Fatal(err)
	}
	if discoveryServer != "" {
		configs[TagDiscoveryServer] = discoveryServer
	}

	if httpListen != "" {
		configs[TagHttpListen] = httpListen
	}
	if nodeListen != "" {
		configs[TagNodeListen] = nodeListen
	}
}

func readConfigFile() error {
	if configFile == "" {
		return nil
	}

	f, err := os.Open(configFile)
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(f)
	err = decoder.Decode(&configs)
	log.Println(configs)
	return err
}
