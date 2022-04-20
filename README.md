***really important - version 2.x.x uses generics, kindly use go 1.18.x and above ***.

> In order to use generics, version 2.x.x uses go 1.18 , kindly check your projects go version before upgrading.

# What is go-mongo-streams?
Go Mongo Streams is a small library for integrating MongoDB Change Stream into a golang project.
This library has been developed to work with GQLGen subscriptions.
It employs a Publisher Subscriber pattern, where multiple subscribers can wait for ChangeStream events of MongoDB.

# How to Use?
Install The Package And Follow The Below Steps.

### 1 ) Create a SubscriptionManager :
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

	subscriptionManager := gomongostreams.NewSubscriptionManager()
```

### 2 ) Create a Publisher :
Create a publisher by calling the GetPublisher function. You need to pass the subscritionManager created before, collection name on which the publisher has to listen and a mongodb filter. If no filter is required, then use "mongo.Pipeline{}" as the second argument.
**As of version 2.0.0, GetPublisher[T] uses generics and type constraint needs to be specified during the function call.**
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

	publisher, err := gomongostreams.GetPublisher[Task](subscriptionManager, taskCollection, pipeline)
```
### 4 ) Subscribe to the Publisher:
The final step is to subscribe to the publisher by calling its Subscribe method of the publisher instance.
```
msg := <-publisher.Subscribe(ctx).Channel
```
### 5 ) Shutdown Streams:
Shutdown the server on demand by calling shutdown method in the subscription manager.
```
subscriptionManager.Shutdown()
```