package pubsubstore

import (
	"fmt"
	"google.golang.org/api/pubsub/v1"
	"sync"
)

func NewStore() *Store {
	return &Store{
		topics: make(map[string]*Topic),
	}
}

type Store struct {
	sync.RWMutex
	topics map[string]*Topic
}

func (m *Store) AddTopic(topicName string) error {
	m.Lock()
	defer m.Unlock()

	_, ok := m.topics[topicName]
	if ok {
		return fmt.Errorf("could not create Topic %s, it already exists", topicName)
	}

	m.topics[topicName] = &Topic{
		id:            topicName,
		subscriptions: make(map[string]*Subscription),
	}

	return nil
}

func (m *Store) GetTopic(topicName string) (*Topic, error) {
	m.RLock()
	defer m.RUnlock()

	t, ok := m.topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic %s, does not exist", topicName)
	}

	return t, nil
}

func (m *Store) FindSub(id string) (*Topic, *Subscription, error) {
	m.RLock()
	defer m.RUnlock()

	// FUTURE: Inefficient to walk whole tree each time
	for _, topic := range m.topics {
		s, ok := topic.subscriptions[id]
		if ok {
			return topic, s, nil
		}
	}

	return nil, nil, fmt.Errorf("no sub with id %s found", id)
}

type Topic struct {
	sync.RWMutex
	id            string
	subscriptions map[string]*Subscription
}

func (t *Topic) AddSub(s *pubsub.Subscription, id string) error {
	t.Lock()
	defer t.Unlock()

	t.subscriptions[id] = &Subscription{
		id:  id,
		url: s.PushConfig.PushEndpoint,
	}
	return nil
}

func (t *Topic) GetSub(id string) (*Subscription, error) {
	t.RLock()
	defer t.RUnlock()

	s, ok := t.subscriptions[id]
	if !ok {
		return nil, fmt.Errorf("sub %s, does not exist", id)
	}

	return s, nil
}

func (t *Topic) RemoveSub(id string) error {
	t.Lock()
	defer t.Unlock()

	_, ok := t.subscriptions[id]
	if ok {
		delete(t.subscriptions, id)
		return nil
	}

	return fmt.Errorf("could not delete sub %s in topic %s because it doesn't exsist", id, t.id)
}

func (t *Topic) GetRoutes() ([]string, error) {
	t.RLock()
	defer t.RUnlock()

	var routeURLs []string
	for _, s := range t.subscriptions {
		routeURLs = append(routeURLs, s.GetURL())
	}
	return routeURLs, nil
}

type Subscription struct {
	sync.RWMutex
	id  string
	url string
}

func (s *Subscription) GetID() string {
	s.RLock()
	defer s.RUnlock()

	return s.id
}

func (s *Subscription) GetURL() string {
	s.RLock()
	defer s.RUnlock()

	return s.url
}