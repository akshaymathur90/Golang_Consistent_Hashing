package main

import (
	"hash/crc32"
	"sort"
	"os"
	"strings"
	"strconv"
	"log"
	"net/http"
	"io/ioutil"
)
func main(){
	//Get nodes from command line
	portRange:= os.Args[1]
	portNums:=strings.Split(portRange,"-")
	//log.Println(portNums[0])
	//log.Println(portNums[1])
	portStart,_:=strconv.ParseInt(portNums[0],10,64)
	portEnd,_:=strconv.ParseInt(portNums[1],10,64)
	log.Println(portStart)
	log.Println(portEnd)
	//Get the key and values to be stored
	inputdata:= os.Args[2]
	keyValPairs:=strings.Split(inputdata,",")
	log.Println(keyValPairs)
	
	myRing:=NewHashRing()
	
	for i:=portStart;i<=portEnd;i++{
		myRing.AddNode(strconv.FormatInt(i,10))
	}
	
	for _,k:= range keyValPairs{
		keyVal:=strings.Split(k,"->")
		log.Println(keyVal[0]+"--"+keyVal[1])
		getNode:=myRing.Get(keyVal[0])
		url:="http://localhost:"+getNode+"/"+keyVal[0]+"/"+keyVal[1]
		log.Println(url)
		req, err :=http.NewRequest("PUT", url,nil)
		req.Header.Set("X-Custom-Header", "myvalue")
    	req.Header.Set("Content-Type", "application/json")

    	client := &http.Client{}
	    resp, err := client.Do(req)
	    if err != nil {
	        panic(err)
	    }
	    defer resp.Body.Close()
	
	    log.Println("response Status:", resp.Status)
	    body, _ := ioutil.ReadAll(resp.Body)
	    log.Println("response Body:", string(body))
	}
}

//Consistent Hashing ring
type NodeRing struct {
  ServerNodes ServerNodes
}
//Array of node servers
type ServerNodes []ServerNode

//Data structure for each server node with hash values
type ServerNode struct {
 Id     string
 HashId uint32
}

//implement functions for custom sorting of the data structure
func (n ServerNodes) Swap(i, j int){ 
      n[i], n[j] = n[j], n[i]
}

//create new node data structure
func NewNode(id string) ServerNode{ 
  return ServerNode{
    Id        : id,
    HashId : crc32.ChecksumIEEE([]byte(id)),
  }
}
func (n ServerNodes) Len() int{
 	return len(n)
 }
//Init new Consistent hashing ring
func NewHashRing() *NodeRing {
  return &NodeRing{ServerNodes : ServerNodes{}}
}
//Add new node to the ring
func (r *NodeRing) AddNode(id string) {
  log.Println(id)
  node := NewNode(id)
  r.ServerNodes = append(r.ServerNodes, node)
  sort.Sort(r.ServerNodes)
}

func (n ServerNodes) Less(i, j int) bool{
	 return n[i].HashId < n[j].HashId
}

//Method to return the node for input key
func (r *NodeRing) Get(key string) string {
  searchfn := func(i int) bool {
  	log.Println(r.ServerNodes[i].HashId)
    return r.ServerNodes[i].HashId >= crc32.ChecksumIEEE([]byte(key))
  }
  i := sort.Search(r.ServerNodes.Len(), searchfn)
  if i >= r.ServerNodes.Len() {
    i = 0
  }
  return r.ServerNodes[i].Id
}

