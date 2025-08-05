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

/*
 * Config
 */
type Config struct {
	MQTTServer string `envconfig:"MQTT_SERVER" default:"mqtt:1883"`
	Topic      string `envconfig:"MQTT_TOPIC"`
	Username   string `envconfig:"MQTT_USERNAME"`
	Password   string `envconfig:"MQTT_PASSWORD"`
}

const (
	TEMP_HUMIDITY_MESSAGE = 56
	WIND_RAIN_MESSAGE     = 49
)

type TempHumidityMeasurement struct {
	Timestamp   string  `json:"time"`
	Temp        float32 `json:"temperature_F"`
	Humidity    float32 `json:"humidity"`
	Battery     int     `json:"battery_ok"`
	MessageType int     `json:"message_type"`
}

type WindRainMeasurement struct {
	Timestamp     string  `json:"time"`
	WindSpeed     float32 `json:"wind_avg_km_h"`
	WindDirection float32 `json:"wind_dir_deg"`
	RainInches    float32 `json:"rain_in"`
	Battery       int     `json:"battery_ok"`
	MessageType   int     `json:"message_type"`
}

/*
 * MQTT Message Handlers
 */
func messagePubHandler(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

func weatherPubHandler(app *App) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received weather message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		var windRainMeasurement WindRainMeasurement

		if err := json.Unmarshal(msg.Payload(), &windRainMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if windRainMeasurement.MessageType == WIND_RAIN_MESSAGE {
			app.SetWindRainConditions(windRainMeasurement)
			return
		}

		var tempHumidityMeasurement TempHumidityMeasurement
		if err := json.Unmarshal(msg.Payload(), &tempHumidityMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if tempHumidityMeasurement.MessageType == TEMP_HUMIDITY_MESSAGE {
			app.SetTempHumidityConditions(tempHumidityMeasurement)
			return
		}

		log.Printf("Unrecognized message type")
	}
}

func connectHandler(client mqtt.Client) {
	log.Println("Connected")
}

func connectAttemptHandler(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
	log.Printf("Attempting connection to %s", broker.Host)
	return tlsCfg
}

func connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Connect lost: %v", err)
}

/*
 * Middleware
 */

func logger(next func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RequestURI, r.RemoteAddr, r.UserAgent())
		next(w, r)
	}
}

/*
  - Rest APP Types
  - {"time":"2025-08-03 21:51:44","model":"Acurite-5n1","message_type":56,
    "id":1026,"channel":"C","sequence_num":0,"battery_ok":1,"wind_avg_km_h":0,
    "temperature_F":69.1,"humidity":97,"mic":"CHECKSUM"}
  - {"time":"2025-08-03 21:52:39","model":"Acurite-5n1","message_type":49,
    "id":1026,"channel":"C","sequence_num":0,"battery_ok":1,"wind_avg_km_h":0,
    "wind_dir_deg":157.5,"rain_in":0.23,"mic":"CHECKSUM"}
*/

type CurrentConditions struct {
	Timestamp     string  `json:"time"`
	Temp          float32 `json:"temperature_F"`
	Humidity      float32 `json:"humidity"`
	Battery       int     `json:"battery_ok"`
	WindSpeed     float32 `json:"wind_avg_km_h"`
	WindDirection float32 `json:"wind_dir_deg"`
	RainInches    float32 `json:"rain_in"`
}

type App struct {
	M                 *sync.Mutex
	currentConditions CurrentConditions
}

func NewApp() *App {
	var mutex sync.Mutex
	app := App{M: &mutex}

	return &app
}

func (app *App) SetTempHumidityConditions(measurement TempHumidityMeasurement) {
	app.M.Lock()
	app.currentConditions.Timestamp = measurement.Timestamp
	app.currentConditions.Temp = measurement.Temp
	app.currentConditions.Humidity = measurement.Humidity
	app.currentConditions.Battery = measurement.Battery
	app.M.Unlock()

}

func (app *App) SetWindRainConditions(measurement WindRainMeasurement) {
	app.M.Lock()
	app.currentConditions.Timestamp = measurement.Timestamp
	app.currentConditions.Battery = measurement.Battery
	app.currentConditions.WindDirection = measurement.WindDirection
	app.currentConditions.WindSpeed = measurement.WindSpeed
	app.currentConditions.RainInches = measurement.RainInches
	app.M.Unlock()
}

func (app *App) GetCurrentConditions() CurrentConditions {
	app.M.Lock()
	m := app.currentConditions
	app.M.Unlock()

	return m
}

func (app *App) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	currentConditions := app.GetCurrentConditions()
	fmt.Fprintf(w, "temperature %f\n"+
		"humidity %f\n"+
		"rain_in %f\n"+
		"wind_direction %f\n"+
		"wind_speed %f\n",
		currentConditions.Temp,
		currentConditions.Humidity,
		currentConditions.RainInches,
		currentConditions.WindDirection,
		currentConditions.WindSpeed,
	)
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

	http.HandleFunc("/metrics", logger(app.MetricsHandler))

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
