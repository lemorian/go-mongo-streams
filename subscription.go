// Package gomongostreams provides a publisher subscriber model for mongo watchstreams.
// The Subscriber interface can be extended to return a channel, which can be used in graphql subscriptions
//Refer to this page to know more about mongo change streams -> https://www.mongodb.com/basics/change-streams
// !Mongo change streams work only on replica servers and not on standalone servers
package gomongostreams

import (
	"context"
	"errors"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//SubscriptionManager holds a map of publishers.
//It creates a key for the publisher map, which is a combination of the collectionName and filter , which allows reuse of publishers.
//! Once instance of Subscription Manager is enough for a Database
type SubscriptionManager[T any] struct {
	publishers map[string]*Publisher[T]
	mu         sync.Mutex
}

//Subscriber interface needs to be implemented to subscribe to the publisher.
//The publisher will call the OnEvent method of the subscriber and provide the data retrived from the mongo change stream.
type Subscriber[T any] struct {
	Channel chan *T
}

func (t *Subscriber[T]) onEvent(data interface{}) error {
	var task T
	bsonBytes, err := bson.Marshal(data)
	if err != nil {
		log.Println(err.Error())
	}
	err = bson.Unmarshal(bsonBytes, &task)
	if err != nil {
		log.Println(err.Error())
	}
	t.Channel <- &task
	return nil
}

//Publisher listens to a changestream even generated on a mongodb collection.
//In order for the publisher to run, it needs a collection object on which it listens and a filter of type mongo.Pipeline which can be used to listen for specific events on the collection.
type Publisher[T any] struct {
	collection  *mongo.Collection
	filter      mongo.Pipeline
	subscribers map[string]*Subscriber[T]
	isListening bool
	stop        chan struct{}
	mu          sync.Mutex
}

//NewSubscriptionManager Creates a new Subscription manager
func NewSubscriptionManager[T any]() *SubscriptionManager[T] {
	return &SubscriptionManager[T]{
		publishers: map[string]*Publisher[T]{},
	}
}

//Shutdown stops all the publishers.
func (s *SubscriptionManager[T]) Shutdown() {
	for _, p := range s.publishers {
		p.stop <- struct{}{}
		p.isListening = false
	}
}

//GetPublisher creates or retrives a Publisher.
//It creates a key for the publisher, which is a combination of the collectionName and filter, which allows reuse of publishers.
//If there is publisher matching the key , a new publisher is created
func (s *SubscriptionManager[T]) GetPublisher(collection *mongo.Collection, filter mongo.Pipeline) (*Publisher[T], error) {
	if collection == nil {
		return nil, errors.New("collection cannot be nil")
	}
	key := collection.Name() + hash(filter) //get the unique key for the mongo filter
	s.mu.Lock()
	//If there is no publisher the key , then create a new publisher and add it to the map
	if s.publishers[key] == nil {
		s.publishers[key] = &Publisher[T]{
			collection:  collection,
			filter:      filter,
			stop:        make(chan struct{}),
			subscribers: make(map[string]*Subscriber[T]),
		}
	}
	s.mu.Unlock()
	return s.publishers[key], nil
}

//Subscribe - publisher will create and add a subscriber to its list and returns it.
//subscriber.Channel can be used to listen to changes
//Remove the subscription by calling cancel() function of the context
func (p *Publisher[T]) Subscribe(ctx context.Context) Subscriber[T] {
	var subscriber Subscriber[T]
	subscriber.Channel = make(chan *T)

	key := randStringRunes(5)
	p.subscribers[key] = &subscriber

	p.mu.Lock()
	if !p.isListening {
		p.startListening()
	}
	p.mu.Unlock()

	//listen for ctx Cancel and remove the subscriber from the publisher
	//If there are no subscribers left then stop listening and close publisher
	go func(ctx context.Context, key string) {
		<-ctx.Done()
		p.mu.Lock()
		delete(p.subscribers, key)
		if len(p.subscribers) == 0 {
			//stop the publisher if there are not subscribers
			p.stop <- struct{}{}
		}
		p.mu.Unlock()

	}(ctx, key)

	return subscriber
}

func (p *Publisher[T]) startListening() {
	p.isListening = true
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := p.collection.Watch(ctx, p.filter, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		log.Printf("error listening to task Change: %s", err)
	}

	//Spawns a listener which listens on the stream
	go func() {
		var event map[string]interface{}
		for stream.Next(ctx) {
			if err = stream.Decode(&event); err != nil {
				log.Printf("error decoding: %s", err)
			}
			err := p.notifyEvent(event["fullDocument"])
			if err != nil {
				log.Printf("error %s", err.Error())
				//? todo close stream
			}
		}
	}()

	//Closes the stream when stop is called
	go func(stop <-chan struct{}) {
		<-stop
		cancel()
		p.isListening = false
	}(p.stop)

}

func (p *Publisher[T]) notifyEvent(data interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, subcriber := range p.subscribers {
		err := (*subcriber).onEvent(data)
		if err != nil {
			return err
		}
	}
	return nil
}
