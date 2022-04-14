package main

import (
	"context"
	"log"
	"time"

	gomongostreams "github.com/lemorian/go-mongo-streams"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Task struct {
	ID          string  `json:"id" bson:"_id"`
	Title       string  `json:"title" bson:"title"`
	Assignee    string  `json:"assignee" bson:"assignee"`
	DueDate     *string `json:"dueDate" bson:"dueDate,omitempty"`
	Description *string `json:"description" bson:"description,omitempty"`
	Status      string  `json:"status" bson:"status"`
	Comment     *string `json:"comment" bson:"comment,omitempty"`
}

func main() {

	var err error
	var ctx, cancel = context.WithCancel(context.Background())

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("DB_URI_HERE").SetMaxPoolSize(1).SetConnectTimeout(15*time.Second))
	if err != nil {
		panic(err)
	}
	instance := client.Database("dev")

	subscriptionManager := gomongostreams.NewSubscriptionManager[Task]()

	tid, err := primitive.ObjectIDFromHex("622b1fd1e59a5f80d8dd5120")
	if err != nil {
		return
	}

	matchID := bson.D{
		{"$match", bson.M{"fullDocument._id": tid}},
	}

	pipeline := mongo.Pipeline{matchID}

	taskCollection := instance.Collection("tasks")

	publisher, err := subscriptionManager.GetPublisher(taskCollection, pipeline)

	if err != nil {
		log.Println(err.Error())
		return
	}

	taskSubscriber := publisher.Subscribe(ctx)

	msg := <-taskSubscriber.Channel
	log.Printf("%v", msg)
	cancel()
	time.Sleep(2 * time.Second)
}
