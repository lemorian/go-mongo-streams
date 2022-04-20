package gomongostreams

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Creates a unique hash has from the pipeline argument, which will be used as the key
func hash(arr mongo.Pipeline) string {
	arrBytes := []byte{}
	for _, item := range arr {
		jsonBytes, _ := json.Marshal(item)
		arrBytes = append(arrBytes, jsonBytes...)
	}
	return fmt.Sprintf("%x", md5.Sum(arrBytes))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
