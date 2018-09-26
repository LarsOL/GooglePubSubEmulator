package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/api/pubsub/v1"
)

// FUTURE make injectable instance
var m Mapping = Mapping{}

func init() {
	m.topics = make(map[string]Topic)
}

type Topic struct {
	Id            string
	subscriptions map[string]Subscription
}

type Subscription struct {
	Id  string
	Url string
}

type Mapping struct {
	sync.RWMutex
	topics map[string]Topic
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func CreateTopic(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	v := mux.Vars(r)
	topicName := v["topicName"]

	if err := addTopic(topicName); err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}
	log.Printf("Added topic: %s \n", topicName)
}

func CreateSub(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	v := mux.Vars(r)
	subId := v["subId"]

	sub := &pubsub.Subscription{}
	err := json.NewDecoder(r.Body).Decode(sub)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	if err := addSub(sub, subId); err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	log.Printf("Added sub: %s to topic; %s with push endpoint: %s \n", subId, sub.Topic, sub.PushConfig.PushEndpoint)
}

func DeleteSub(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	subId := v["subId"]

	if err := removeSub(subId); err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}
	log.Printf("Deleted sub: %s \n", subId)
}

type MessageType struct {
	Attributes map[string]string `json:"attributes"`
	Data       string            `json:"data"`
	MessageID  string            `json:"message_id"`
}
type PushMessage struct {
	Message      MessageType `json:"message"`
	Subscription string      `json:"subscription"`
}

func PublishToTopic(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	v := mux.Vars(r)
	topic := v["topic"]

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	payload := &pubsub.PublishRequest{}
	err = json.Unmarshal(b, payload)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	routes, err := getRoutes(topic)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	for _, msg := range payload.Messages {
		pushMsg := PushMessage{
			Message: MessageType{
				msg.Attributes,
				msg.Data,
				RandStringRunes(5),
			},
			Subscription: "Subscription",
		}
		marshaledMsg, err := json.Marshal(pushMsg)
		if err != nil {
			HttpErrorLog(w, err, http.StatusBadRequest)
			return
		}
		for _, route := range routes {
			go sendToRoute(route, marshaledMsg)
		}
	}

	log.Printf("Published to topic: %s \n", topic)
	// technically should return messageIds
	w.WriteHeader(http.StatusNoContent)
}

func sendToRoute(url string, msg []byte) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(msg))
	if err != nil {
		log.Printf("Could not complete request to %s got err %s\n", url, err.Error())
		return
	}
	if resp.StatusCode != 200 {
		log.Printf("Serivce on %s returned status %d, THIS WOULD NORMALLY RETRY got err %s\n", url, resp.StatusCode, err.Error())
		return
	}
	log.Printf("Message sent to %s with len %d %v\n", url, len(msg), string(msg))
}

func addTopic(topicName string) error {
	m.Lock()
	defer m.Unlock()
	
	_, ok := m.topics[topicName]
	if ok {
		return fmt.Errorf("could not create Topic %s, it already exists", topicName)
	}

	m.topics[topicName] = Topic{
		Id:            topicName,
		subscriptions: make(map[string]Subscription),
	}

	return nil
}

const topicPrefix = "projects/localhost/topics/"

func addSub(s *pubsub.Subscription, id string) error {
	m.Lock()
	defer m.Unlock()

	shortTopicName := s.Topic[len(topicPrefix):]
	topic, ok := m.topics[shortTopicName]
	if !ok {
		return fmt.Errorf("could not create sub %s Topic %s does not exist", id, shortTopicName)

	}

	topic.subscriptions[id] = Subscription{
		Id:  id,
		Url: s.PushConfig.PushEndpoint,
	}
	return nil
}

func removeSub(Id string) error {
	m.Lock()
	defer m.Unlock()

	for _, topic := range m.topics {
		_, ok := topic.subscriptions[Id]
		if ok {
			delete(topic.subscriptions, Id)
			return nil
		}
	}

	return fmt.Errorf("Could not delete sub %s because it doesn't exsist", Id)
}

func getRoutes(topic string) ([]string, error) {
	m.RLock()
	defer m.RUnlock()

	t, ok := m.topics[topic]
	if !ok {
		return nil, fmt.Errorf("Could not get route info for topic %s because it doesn't exsist", topic)
	}

	routeURLs := []string{}
	for _, v := range t.subscriptions {
		routeURLs = append(routeURLs, v.Url)
	}
	return routeURLs, nil
}

func Health(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte("pub sub is running OK!!"))
}

func RunServer(addr string) {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost"+addr)
	r := mux.NewRouter()
	r.Methods(http.MethodPut).Path("/v1/projects/localhost/topics/{topicName}").HandlerFunc(CreateTopic)
	r.Methods(http.MethodPut).Path("/v1/projects/localhost/subscriptions/{subId}").HandlerFunc(CreateSub)
	r.Methods(http.MethodDelete).Path("/v1/projects/localhost/subscriptions/{subId}").HandlerFunc(DeleteSub)
	r.Methods(http.MethodPost).Path("/v1/projects/localhost/topics/{topic}:publish").HandlerFunc(PublishToTopic)
	r.Methods(http.MethodGet).Path("/v1/projects/localhost/subscriptions").HandlerFunc(Health)
	log.Fatal(http.ListenAndServe(addr, r))
}

func main() {
	RunServer(":8010")
}

func HttpErrorLog(w http.ResponseWriter, err error, code int) {
	log.Println(err.Error())
	http.Error(w, err.Error(), code)
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
