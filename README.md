# What is go-mongo-streams?
Go Mongo Streams is a small library for integrating MongoDB Change Stream into a golang project.
This library has been developed to work with GQLGen subscriptions.
It employs a Publisher Subscriber pattern, where multiple subscribers can wait for ChangeStream events of MongoDB.

# How to Use?
Install The Package And Follow The Below Steps.

###### 1 ) Create a SubscriptionManager :
Create a new Subscription Manager using the NewSubscriptionManager function. It takes in a pointer of a mongodb database as an argument. Typically one Subscription Manager is enough for a mongodb database instance.
A Subscription manager holds multiple publishers. Each of which is responsible for listening to a single change stream event.
```go
var err error
	var ctx, cancel = context.WithCancel(context.Background())

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("dbURI").SetMaxPoolSize(1).SetConnectTimeout(15*time.Second))
	if err != nil {
		panic(err)
	}
	instance := client.Database("dbName")

	subscriptionManager := gomongostreams.NewSubscriptionManager(instance)
```

###### 2 ) Create a Publisher :
Create a publisher by calling the GetPublisher function on the subscription manager instance. You need to pass the collection name on which the publisher has to listen and a mongodb filter. If no filter is required, then use "mongo.Pipeline{}" as the second argument.
Note: GetPublisher is idempotent, and would return the same publisher instance for the same collection name and filter combination. This allows reusing the same publisher to serve multiple subscribers, listening for the same data.
```go
tid, err := primitive.ObjectIDFromHex("60e8eecdea69f2f6cf10530f")
	if err != nil {
		return
	}

	matchID := bson.D{
		{"$match", bson.M{"fullDocument._id": tid}},
	}

	pipeline := mongo.Pipeline{matchID}

	publisher := subscriptionManager.GetPublisher("tasks", pipeline)
```
###### 3 ) Implement the Subscriber interface:
To subscribe to the publisher, you need a Struct that implements the Subscriber interface. And this allows the publisher to call the "OnEvent" function of the subscriber interface when there is a new event.

```go
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
```
###### 4 )Subscribe to the Publisher:
The final step is to subscribe to the publisher by calling its Subscribe method and passing in the subscriber instance.



