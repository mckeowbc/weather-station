package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/kelseyhightower/envconfig"
	weathermetrics "github.com/mckeowbc/weather-metrics"
)

func weatherPubHandler(app *App) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received weather message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		var windRainMeasurement weathermetrics.WindRainMeasurement

		if err := json.Unmarshal(msg.Payload(), &windRainMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if windRainMeasurement.MessageType == weathermetrics.WIND_RAIN_MESSAGE {
			app.SetWindRainConditions(windRainMeasurement)
			return
		}

		var tempHumidityMeasurement weathermetrics.TempHumidityMeasurement
		if err := json.Unmarshal(msg.Payload(), &tempHumidityMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if tempHumidityMeasurement.MessageType == weathermetrics.TEMP_HUMIDITY_MESSAGE {
			app.SetTempHumidityConditions(tempHumidityMeasurement)
			return
		}

		log.Printf("Unrecognized message type")
	}
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

type App struct {
	M                 *sync.Mutex
	currentConditions weathermetrics.CurrentConditions
}

func NewApp() *App {
	var mutex sync.Mutex
	app := App{M: &mutex}

	return &app
}

func (app *App) SetTempHumidityConditions(measurement weathermetrics.TempHumidityMeasurement) {
	app.M.Lock()
	app.currentConditions.Timestamp = measurement.Timestamp
	app.currentConditions.Temp = measurement.Temp
	app.currentConditions.Humidity = measurement.Humidity
	app.currentConditions.Battery = measurement.Battery
	app.M.Unlock()

}

func (app *App) SetWindRainConditions(measurement weathermetrics.WindRainMeasurement) {
	app.M.Lock()
	app.currentConditions.Timestamp = measurement.Timestamp
	app.currentConditions.Battery = measurement.Battery
	app.currentConditions.WindDirection = measurement.WindDirection
	app.currentConditions.WindSpeed = measurement.WindSpeed
	app.currentConditions.RainInches = measurement.RainInches
	app.M.Unlock()
}

func (app *App) GetCurrentConditions() weathermetrics.CurrentConditions {
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
	var conf weathermetrics.MQTTConfig
	if err := envconfig.Process("weather", &conf); err != nil {
		log.Fatal(err)
	}

	if len(conf.Username) > 0 && len(conf.Password) == 0 ||
		len(conf.Username) == 0 && len(conf.Password) > 0 {
		log.Fatal("Error: Must specify both username and password")
	}

	client, _ := weathermetrics.NewMQTTClient(conf)

	app := NewApp()

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
