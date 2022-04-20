// Package gomongostreams provides a publisher subscriber model for mongo "watchstreams".
// The Subscriber interface can be extended to return a channel, which can be used in graphql subscriptions
// Refer to this page to know more about mongo change streams -> https://www.mongodb.com/basics/change-streams
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

// Constants
const randNumberSeed = 5

// Error Constants
var (
	errCollectionNotFound   = errors.New("collection cannot be nil")
	errPublishedFetchFailed = errors.New("fetching publisher failed")
)

// SubscriptionManager holds a map of publishers.
// It creates a key for the publisher map,
// which is a combination of the collectionName and filter , which allows reuse of publishers.
// !Once instance of Subscription Manager is enough for a Database
type SubscriptionManager struct {
	publishers map[string]Publisher
	mu         sync.Mutex
}

// Subscriber interface needs to be implemented to subscribe to the publisher.
// The publisher will call the OnEvent method of the subscriber and
// provide the data retrieved from the mongo change stream.
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

// Publisher listens to a changestream event generated on a mongodb collection.
// In order for the publisher to run,
// it needs a collection object on which it listens and
// a filter of type mongo.Pipeline which can be used to listen for specific events on the collection.
type Publisher interface {
	startListening()
	stopListening()
	notifyEvent(data interface{}) error
}

type PublisherStruct[T any] struct {
	collection  *mongo.Collection
	filter      mongo.Pipeline
	subscribers map[string]*Subscriber[T]
	isListening bool
	stop        chan struct{}
	mu          sync.Mutex
}

// NewSubscriptionManager Creates a new Subscription manager
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		publishers: map[string]Publisher{},
		mu:         sync.Mutex{},
	}
}

// Shutdown stops all the publishers.
func (s *SubscriptionManager) Shutdown() {
	for _, p := range s.publishers {
		p.stopListening()
	}
}

// GetPublisher creates or retrieves a Publisher.
// It creates a key for the publisher,
// which is a combination of the collectionName and filter, which allows reuse of publishers.
// If there is no publisher matching the key , a new publisher is created
func GetPublisher[DataType any](subscriptionManager *SubscriptionManager,
	collection *mongo.Collection,
	filter mongo.Pipeline,
) (*PublisherStruct[DataType], error) {
	if collection == nil {
		return nil, errCollectionNotFound
	}

	key := collection.Name() + hash(filter) // get the unique key for the mongo filter

	subscriptionManager.mu.Lock()
	// If there is no publisher the key , then create a new publisher and add it to the map
	if subscriptionManager.publishers[key] == nil {
		subscriptionManager.publishers[key] = &PublisherStruct[DataType]{
			collection:  collection,
			filter:      filter,
			stop:        make(chan struct{}),
			subscribers: make(map[string]*Subscriber[DataType]),
		}
	}
	subscriptionManager.mu.Unlock()

	if publisher, ok := subscriptionManager.publishers[key].(*PublisherStruct[DataType]); ok {
		return publisher, nil
	}

	return nil, errPublishedFetchFailed
}

// Subscribe - publisher will create and add a subscriber to its list and returns it.
// subscriber.Channel can be used to listen to changes
// Remove the subscription by calling cancel() function of the context
func (t *PublisherStruct[T]) Subscribe(ctx context.Context) Subscriber[T] {
	var subscriber Subscriber[T]
	subscriber.Channel = make(chan *T)

	key := randStringRunes(randNumberSeed)
	t.subscribers[key] = &subscriber

	t.mu.Lock()
	if !t.isListening {
		t.startListening()
	}
	t.mu.Unlock()

	// listen for ctx Cancel and remove the subscriber from the t
	// If there are no subscribers left then stop listening and close t
	go func(ctx context.Context, key string) {
		<-ctx.Done()
		t.mu.Lock()
		delete(t.subscribers, key)

		if len(t.subscribers) == 0 {
			// stop the tt if there are no subscribers
			t.stop <- struct{}{}
		}

		t.mu.Unlock()
	}(ctx, key)

	return subscriber
}

func (t *PublisherStruct[T]) startListening() {
	t.isListening = true
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := t.collection.Watch(ctx,
		t.filter,
		options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		log.Printf("error listening to task Change: %s", err)
	}

	// Spawns a listener which listens on the stream
	go func() {
		var event map[string]interface{}
		for stream.Next(ctx) {
			if err = stream.Decode(&event); err != nil {
				log.Printf("error decoding: %s", err)
			}

			err := t.notifyEvent(event["fullDocument"])
			if err != nil {
				// ?todo close stream
				log.Printf("error %s", err.Error())
			}
		}
	}()

	// Closes the stream when stop is called
	go func(stop <-chan struct{}) {
		<-stop
		cancel()

		t.isListening = false
	}(t.stop)
}

func (t *PublisherStruct[T]) notifyEvent(data interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, subscriber := range t.subscribers {
		err := (*subscriber).onEvent(data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *PublisherStruct[T]) stopListening() {
	t.stop <- struct{}{}
	t.isListening = false
}
