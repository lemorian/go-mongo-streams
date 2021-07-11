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

type TaskSubscriber struct {
	channel chan *Task
}

func (t *TaskSubscriber) OnEvent(data interface{}) error {
	var task = Task{}
	bsonBytes, err := bson.Marshal(data)
	if err != nil {
		log.Println(err.Error())
	}
	err = bson.Unmarshal(bsonBytes, &task)
	if err != nil {
		log.Println(err.Error())
	}
	t.channel <- &task
	return nil
}

func main() {

	var err error

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongoDBURL").SetMaxPoolSize(1).SetConnectTimeout(15*time.Second))
	if err != nil {
		panic(err)
	}
	instance := client.Database("dbName")

	subscriptionManager := gomongostreams.NewSubscriptionManager(instance)

	tid, err := primitive.ObjectIDFromHex("ID")
	if err != nil {
		return
	}

	matchID := bson.D{
		{"$match", bson.M{"fullDocument._id": tid}},
	}

	pipeline := mongo.Pipeline{matchID}

	publisher := subscriptionManager.GetPublisher("tasks", pipeline)

	var taskSubscriber gomongostreams.Subscriber = &TaskSubscriber{
		channel: make(chan *Task),
	}
	publisher.Subscribe(&taskSubscriber)

	msg := <-taskSubscriber.(*TaskSubscriber).channel
	log.Printf("%v", msg)
	subscriptionManager.Shutdown()

}
