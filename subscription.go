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

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//SubscriptionManager holds a map of publishers.
//It creates a key for the publisher map, which is a combination of the collectionName and filter , which allows reuse of publishers.
//! Once instance of Subscription Manager is enough for a Database
type SubscriptionManager struct {
	publishers map[string]*Publisher
	mu         sync.Mutex
}

//Subscriber interface needs to be implemented to subscribe to the publisher.
//The publisher will call the OnEvent method of the subscriber and provide the data retrived from the mongo change stream.
type Subscriber interface {
	OnEvent(data interface{}) error
}

//Publisher listens to a changestream even generated on a mongodb collection.
//In order for the publisher to run, it needs a collection object on which it listens and a filter of type mongo.Pipeline which can be used to listen for specific events on the collection.
type Publisher struct {
	collection  *mongo.Collection
	filter      mongo.Pipeline
	subscribers map[string]*Subscriber
	isListening bool
	stop        chan struct{}
	mu          sync.Mutex
}

//NewSubscriptionManager Creates a new Subscription manager
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		publishers: map[string]*Publisher{},
	}
}

//Shutdown stops all the publishers.
func (s *SubscriptionManager) Shutdown() {
	for _, p := range s.publishers {
		p.stop <- struct{}{}
		p.isListening = false
	}
}

//GetPublisher creates or retrives a Publisher.
//It creates a key for the publisher, which is a combination of the collectionName and filter, which allows reuse of publishers.
//If there is publisher matching the key , a new publisher is created
func (s *SubscriptionManager) GetPublisher(collection *mongo.Collection, filter mongo.Pipeline) (*Publisher, error) {
	if collection == nil {
		return nil, errors.New("collection cannot be nil")
	}
	key := collection.Name() + hash(filter) //get the unique key for the mongo filter
	s.mu.Lock()
	//If there is no publisher the key , then create a new publisher and add it to the map
	if s.publishers[key] == nil {
		s.publishers[key] = &Publisher{
			collection:  collection,
			filter:      filter,
			stop:        make(chan struct{}),
			subscribers: make(map[string]*Subscriber),
		}
	}
	s.mu.Unlock()
	return s.publishers[key], nil
}

//Subscribe - publisher will add the @subscriber to its list and notifies on new events by calling OnEvent method of the subscriber
//The subscriber should implement the Subscriber interface, refer to  examples for more information.
//Remove the subscription by calling cancel() function of the context
func (p *Publisher) Subscribe(ctx context.Context, subscriber *Subscriber) {
	key := randStringRunes(5)
	p.subscribers[key] = subscriber

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
}

func (p *Publisher) startListening() {
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

func (p *Publisher) notifyEvent(data interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, subcriber := range p.subscribers {
		err := (*subcriber).OnEvent(data)
		if err != nil {
			return err
		}
	}
	return nil
}
