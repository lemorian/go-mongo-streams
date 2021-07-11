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

//Task - Task Model stored in mongodb task Collection
type Task struct {
	ID          string  `json:"id" bson:"_id"`
	Title       string  `json:"title" bson:"title"`
	Assignee    string  `json:"assignee" bson:"assignee"`
	DueDate     *string `json:"dueDate" bson:"dueDate,omitempty"`
	Description *string `json:"description" bson:"description,omitempty"`
	Status      string  `json:"status" bson:"status"`
	Comment     *string `json:"comment" bson:"comment,omitempty"`
}

//TaskSubscriber - Subscribes to the Task Changes from Publisher
type TaskSubscriber struct {
	channel chan *Task
}

//OnEvent is called by the Publisher whenever a new event occurs
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
	var ctx, cancel = context.WithCancel(context.Background())

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb+srv://dba:btrAVOph1B8E9k5F@dev.ttafq.mongodb.net/myFirstDatabase?retryWrites=true&w=majority").SetMaxPoolSize(1).SetConnectTimeout(15*time.Second))
	if err != nil {
		panic(err)
	}
	instance := client.Database("dev")

	subscriptionManager := gomongostreams.NewSubscriptionManager()

	tid, err := primitive.ObjectIDFromHex("60e8eecdea69f2f6cf10530f")
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

	var taskSubscriber gomongostreams.Subscriber = &TaskSubscriber{
		channel: make(chan *Task),
	}
	publisher.Subscribe(ctx, &taskSubscriber)

	msg := <-taskSubscriber.(*TaskSubscriber).channel
	log.Printf("%v", msg)
	cancel()
	time.Sleep(2 * time.Second)
}
