// Package gomongostreams provides a publisher subscriber model for mongo watchstreams.
// The Subscriber interface can be extended to return a channel, which can be used in graphql subscriptions
//Refer to this page to know more about mongo change streams -> https://www.mongodb.com/basics/change-streams
// !Mongo change streams work only on replica servers and not on standalone servers
package gomongostreams

import (
	"context"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//SubscriptionManager holds a pointer to a mongodb database and a map of publishers.
//It creates a key for the publisher map, which is a combination of the collectionName and filter , which allows reuse of publishers.
type subscriptionManager struct {
	publishers map[string]*Publisher
	db         *mongo.Database
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
	subscribers []*Subscriber
	isListening bool
	stop        chan bool
	mu          sync.Mutex
}

//NewSubscriptionManager Creates a new Subscription manager
func NewSubscriptionManager(db *mongo.Database) *subscriptionManager {
	return &subscriptionManager{
		publishers: map[string]*Publisher{},
		db:         db,
	}
}

//Shutdown stops all the publishers.
func (s *subscriptionManager) Shutdown() {
	for _, p := range s.publishers {
		p.stop <- true
	}
}

//GetPublisher creates or retrives a Publisher.
//It creates a key for the publisher, which is a combination of the collectionName and filter, which allows reuse of publishers.
//If there is publisher matching the key , a new publisher is created
func (s *subscriptionManager) GetPublisher(collectionName string, filter mongo.Pipeline) *Publisher {
	key := collectionName + hash(filter) //get the unique key for the mongo filter

	//If there is no publisher the key , then create a new publisher and add it to the map
	if s.publishers[key] == nil {
		s.publishers[key] = &Publisher{
			collection: s.db.Collection(collectionName),
			filter:     filter,
		}
	}

	return s.publishers[key]
}

//Subscribe - publisher will add the @subscriber to its list and notifies on new events by calling OnEvent method of the subscriber
//The subscriber should implement the Subscriber interface, refer to  examples for more information.
func (p *Publisher) Subscribe(subscriber *Subscriber) {
	p.mu.Lock()
	p.subscribers = append(p.subscribers, subscriber)

	if !p.isListening {
		p.startListening()
	}
	p.mu.Unlock()
}

func (p *Publisher) startListening() {
	ctx := context.Background()

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
				//todo close stream?
			}
		}
	}()

	//Closes the stream when stop is called
	go func() {
		p.mu.Lock()
		<-p.stop
		stream.Close(ctx)
		p.isListening = false
		p.mu.Unlock()
	}()

}

func (p *Publisher) notifyEvent(data interface{}) error {
	for _, subcriber := range p.subscribers {
		err := (*subcriber).OnEvent(data)
		if err != nil {
			return err
		}
	}
	return nil
}
