package gomongostreams

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

//Creates a unique hash has from the pipeline argument, which will be used as the key
func hash(arr mongo.Pipeline) string {
	arrBytes := []byte{}
	for _, item := range arr {
		jsonBytes, _ := json.Marshal(item)
		arrBytes = append(arrBytes, jsonBytes...)
	}
	return fmt.Sprintf("%x", md5.Sum(arrBytes))
}
