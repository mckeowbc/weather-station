package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/kelseyhightower/envconfig"
	weathermetrics "github.com/mckeowbc/weather-metrics"
)

const URL = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"

func handleWindRainMeasurement(m weathermetrics.WindRainMeasurement) map[string]string {
	w := map[string]string{
		// "timestamp":   m.Timestamp,
		"windspeedmph": fmt.Sprintf("%0.2f", m.WindSpeed*621371),
		"wind_dir":     fmt.Sprintf("%0.2f", m.WindDirection),
		"dailyrainin":  fmt.Sprintf("%0.2f", m.RainInches),
	}

	return w
}

func handleTempHumidityMeasurement(m weathermetrics.TempHumidityMeasurement) map[string]string {
	w := map[string]string{
		// "timestamp": m.Timestamp,
		"tempf":    fmt.Sprintf("%0.2f", m.Temp),
		"humidity": fmt.Sprintf("%0.2f", m.Humidity),
	}

	return w
}

func weatherPubHandler(c chan<- map[string]string) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received weather message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		var windRainMeasurement weathermetrics.WindRainMeasurement

		if err := json.Unmarshal(msg.Payload(), &windRainMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if windRainMeasurement.MessageType == weathermetrics.WIND_RAIN_MESSAGE {
			c <- handleWindRainMeasurement(windRainMeasurement)
			return
		}

		var tempHumidityMeasurement weathermetrics.TempHumidityMeasurement
		if err := json.Unmarshal(msg.Payload(), &tempHumidityMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if tempHumidityMeasurement.MessageType == weathermetrics.TEMP_HUMIDITY_MESSAGE {
			c <- handleTempHumidityMeasurement(tempHumidityMeasurement)
			return
		}

		log.Printf("ERROR: Unrecognized message type")
	}
}

func main() {
	key := flag.String("key", "", "PWS Key")
	id := flag.String("id", "", "PWS ID")
	flag.Parse()

	var conf weathermetrics.MQTTConfig
	if err := envconfig.Process("weather", &conf); err != nil {
		log.Fatal(err)
	}

	if len(conf.Username) > 0 && len(conf.Password) == 0 ||
		len(conf.Username) == 0 && len(conf.Password) > 0 {
		log.Fatal("Error: Must specify both username and password")
	}

	client, _ := weathermetrics.NewMQTTClient(conf)

	log.Printf("Connecting to %s", fmt.Sprintf("tcp://%s", conf.MQTTServer))

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	c := make(chan map[string]string)
	sub(client, conf.Topic, weatherPubHandler(c))
	defer close(client, conf.Topic)

	timer := time.After(time.Second * 60)

	data := make(map[string]string)

	// Wait for interrupt signal to gracefully shutdown the subscriber
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

outerloop:
	for {
		select {
		case msg := <-c:

			for key := range msg {
				data[key] = msg[key]
			}

		case <-timer:
			resp, err := submitMeasurement(*id, *key, data)
			defer resp.Body.Close()
			if err != nil {
				log.Println(err)
			} else {
				body, _ := io.ReadAll(resp.Body)
				log.Printf("%d %s", resp.StatusCode, body)
			}
			timer = time.After(time.Second * 60)
		case <-sigChan:
			break outerloop
		}
	}
}

func submitMeasurement(id, key string, values map[string]string) (*http.Response, error) {
	mdict := map[string]string{
		"ID":       id,
		"PASSWORD": key,
		"action":   "updateraw",
		"dateutc":  "now",
	}

	for k := range values {
		mdict[k] = values[k]
	}

	queryParams := []string{}

	for k := range mdict {
		queryParams = append(queryParams, fmt.Sprintf("%s=%s", k, mdict[k]))
	}

	queryString := strings.Join(queryParams, "&")
	log.Println(URL + "?" + queryString)
	return http.Get(URL + "?" + queryString)
}

func sub(client mqtt.Client, topic string, handler mqtt.MessageHandler) {
	token := client.Subscribe(topic, 1, handler)
	token.Wait()
	log.Printf("Subscribed to topic: %s", topic)
}

func close(client mqtt.Client, topic string) {
	client.Unsubscribe(topic)
	client.Disconnect(250)
}
