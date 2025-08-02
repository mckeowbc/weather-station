package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	MQTTServer string `envconfig:"MQTT_SERVER" default:"mqtt:1883"`
	Topic      string `envconfig:"MQTT_TOPIC"`
	Username   string `envconfig:"MQTT_USERNAME"`
	Password   string `envconfig:"MQTT_PASSWORD"`
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

func weatherPubHandler(app *App) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received weather message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		var currentConditions WeatherMeasurement
		if err := json.Unmarshal(msg.Payload(), &currentConditions); err != nil {
			log.Printf("Could not decode weather data: %s", err)
			return
		}

		app.SetCurrentConditions(currentConditions)
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	log.Println("Connected")
}

var connectAttemptHandler mqtt.ConnectionAttemptHandler = func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
	log.Printf("Attempting connection to %s", broker.Host)
	return tlsCfg
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Printf("Connect lost: %v", err)
}

type WeatherMeasurement struct {
	Temp     int `json:"temp"`
	Humidity int `json:"humidity"`
}

type App struct {
	M                 *sync.Mutex
	currentConditions WeatherMeasurement
}

func NewApp() *App {
	var mutex sync.Mutex
	app := App{M: &mutex}

	return &app
}

func (app *App) SetCurrentConditions(measurement WeatherMeasurement) {
	app.M.Lock()
	app.currentConditions = measurement
	app.M.Unlock()
}

func (app *App) GetCurrentConditions() WeatherMeasurement {
	app.M.Lock()
	m := app.currentConditions
	app.M.Unlock()

	return m
}

func (app *App) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("/metrics %s", r.UserAgent())
	currentConditions := app.GetCurrentConditions()
	fmt.Fprintf(w, "temperature %d\nhumidity %d\n", currentConditions.Temp, currentConditions.Humidity)
}

func main() {
	var conf Config
	if err := envconfig.Process("weather", &conf); err != nil {
		log.Fatal(err)
	}

	if len(conf.Username) > 0 && len(conf.Password) == 0 ||
		len(conf.Username) == 0 && len(conf.Password) > 0 {
		log.Fatal("Error: Must specify both username and password")
	}

	app := NewApp()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", conf.MQTTServer))
	opts.SetClientID("weather-metrics")
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(time.Second * 2)
	opts.SetConnectionAttemptHandler(connectAttemptHandler)
	if len(conf.Username) > 0 {
		opts.SetUsername(conf.Username)
		opts.SetPassword(conf.Password)
	}

	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)

	log.Printf("Connecting to %s", fmt.Sprintf("tcp://%s", conf.MQTTServer))

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if len(conf.Topic) > 0 {
		sub(client, conf.Topic, weatherPubHandler(app))
	}

	http.HandleFunc("/metrics", app.MetricsHandler)

	log.Print("HTTP Listening on :8080")
	err := http.ListenAndServe(":8080", nil)
	log.Fatal(err)

	// Wait for interrupt signal to gracefully shutdown the subscriber
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Unsubscribe and disconnect
	fmt.Println("Unsubscribing and disconnecting...")

	if len(conf.Topic) > 0 {
		client.Unsubscribe(conf.Topic)
	}
	client.Disconnect(250)

}

func sub(client mqtt.Client, topic string, handler mqtt.MessageHandler) {
	token := client.Subscribe(topic, 1, handler)
	token.Wait()
	log.Printf("Subscribed to topic: %s", topic)
}
