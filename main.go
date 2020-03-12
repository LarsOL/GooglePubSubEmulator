package main

import (
	"bytes"
	"encoding/json"
	"github.com/LarsOL/GooglePubSubEmulator/pubsubstore"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/api/pubsub/v1"
)

var defaultStore *pubsubstore.Store

func init() {
	rand.Seed(time.Now().UnixNano())
	defaultStore = pubsubstore.NewStore()
}

func CreateTopic(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	v := mux.Vars(r)
	topicName := v["topicName"]

	if err := defaultStore.AddTopic(topicName); err != nil {
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

	shortTopicName := sub.Topic[len(topicPrefix):]
	t, err := defaultStore.GetTopic(shortTopicName)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	if err := t.AddSub(sub, subId); err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	log.Printf("Added sub: %s to topic; %s with push endpoint: %s \n", subId, sub.Topic, sub.PushConfig.PushEndpoint)
}

func DeleteSub(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	subId := v["subId"]


	t, s, err := defaultStore.FindSub(subId)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	if err := t.RemoveSub(s.GetID()); err != nil {
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

	t, err := defaultStore.GetTopic(topic)
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	routes, err := t.GetRoutes()
	if err != nil {
		HttpErrorLog(w, err, http.StatusBadRequest)
		return
	}

	for _, msg := range payload.Messages {
		pushMsg := PushMessage{
			Message: MessageType{
				msg.Attributes,
				msg.Data,
				RandString(5),
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
		log.Printf("Serivce on %s returned status %d, THIS WOULD NORMALLY RETRY \n", url, resp.StatusCode)
		return
	}
	log.Printf("Message sent to %s with len %d %v\n", url, len(msg), string(msg))
}

const topicPrefix = "projects/localhost/topics/"


func Health(w http.ResponseWriter, _ *http.Request) {
	_,_ = w.Write([]byte("pub sub is running OK!!"))
}

func RunServer(port string) {
	addr := ":" + port
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost"+addr)
	r := mux.NewRouter()
	r.Methods(http.MethodPut).Path("/v1/projects/localhost/topics/{topicName}").HandlerFunc(CreateTopic)
	r.Methods(http.MethodPut).Path("/v1/projects/localhost/subscriptions/{subId}").HandlerFunc(CreateSub)
	r.Methods(http.MethodDelete).Path("/v1/projects/localhost/subscriptions/{subId}").HandlerFunc(DeleteSub)
	r.Methods(http.MethodPost).Path("/v1/projects/localhost/topics/{topic}:publish").HandlerFunc(PublishToTopic)
	r.Methods(http.MethodGet).Path("/v1/projects/localhost/subscriptions").HandlerFunc(Health)
	log.Printf("Starting server on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8010"
	}
	RunServer(port)
}

func HttpErrorLog(w http.ResponseWriter, err error, code int) {
	log.Println(err.Error())
	http.Error(w, err.Error(), code)
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
