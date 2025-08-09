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

type RTL433Message struct {
	Timestamp *time.Time
	Data      map[string]string
}

func (a *App) parseMessageTime(timestamp string) (*time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", timestamp, a.TZ)

	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (a *App) handleWindRainMeasurement(m weathermetrics.WindRainMeasurement) map[string]string {
	t := time.Now().In(a.TZ)

	if t.Hour() == 0 && t.Minute() == 0 {
		a.LastRainFall = -1.0
	}

	if a.LastRainFall < 0 {
		a.LastRainFall = m.RainInches
	}

	return map[string]string{
		"windspeedmph": fmt.Sprintf("%0.2f", m.WindSpeed*0.62137119),
		"wind_dir":     fmt.Sprintf("%0.2f", m.WindDirection),
		"dailyrainin":  fmt.Sprintf("%0.2f", m.RainInches-a.LastRainFall),
	}
}

func handleTempHumidityMeasurement(m weathermetrics.TempHumidityMeasurement) map[string]string {
	return map[string]string{
		"tempf":    fmt.Sprintf("%0.2f", m.Temp),
		"humidity": fmt.Sprintf("%0.2f", m.Humidity),
	}
}

func (a *App) weatherPubHandler(c chan<- RTL433Message) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received weather message: %s from topic: %s\n", msg.Payload(), msg.Topic())

		var windRainMeasurement weathermetrics.WindRainMeasurement

		if err := json.Unmarshal(msg.Payload(), &windRainMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		timestamp, err := a.parseMessageTime(windRainMeasurement.Timestamp)
		if err != nil {
			log.Printf("could not parse timestamp %s: %s", windRainMeasurement.Timestamp, err)
			return
		}

		if windRainMeasurement.MessageType == weathermetrics.WIND_RAIN_MESSAGE {
			c <- RTL433Message{
				Timestamp: timestamp,
				Data:      a.handleWindRainMeasurement(windRainMeasurement),
			}
			return
		}

		var tempHumidityMeasurement weathermetrics.TempHumidityMeasurement
		if err := json.Unmarshal(msg.Payload(), &tempHumidityMeasurement); err != nil {
			log.Printf("Could not decode json data: %s", err)
			return
		}

		if tempHumidityMeasurement.MessageType == weathermetrics.TEMP_HUMIDITY_MESSAGE {
			c <- RTL433Message{
				Timestamp: timestamp,
				Data:      handleTempHumidityMeasurement(tempHumidityMeasurement),
			}
			return
		}

		log.Printf("ERROR: Unrecognized message type")
	}
}

type App struct {
	LastRainFall float32
	TZ           *time.Location
}

func NewApp(tz string) (App, error) {
	timezone, err := time.LoadLocation(tz)
	if err != nil {
		return App{}, err
	}

	return App{LastRainFall: -1.0, TZ: timezone}, nil
}

type PWSConfig struct {
	Key string
	ID  string
	TZ  string `default:"America/New_York"`
}

func main() {
	key := flag.String("key", "", "PWS Key")
	id := flag.String("id", "", "PWS ID")
	flag.Parse()

	var mqttConf weathermetrics.MQTTConfig
	if err := envconfig.Process("weather", &mqttConf); err != nil {
		log.Fatal(err)
	}

	if len(mqttConf.Username) > 0 && len(mqttConf.Password) == 0 ||
		len(mqttConf.Username) == 0 && len(mqttConf.Password) > 0 {
		log.Fatal("Error: Must specify both username and password")
	}

	var pwsConf PWSConfig
	if err := envconfig.Process("pws", &pwsConf); err != nil {
		log.Fatal(err)
	}

	if *key == "" {
		*key = pwsConf.Key
	}

	if *id == "" {
		*id = pwsConf.ID
	}

	if *key == "" || *id == "" {
		log.Fatal("Must set PWS_KEY and PWS_ID")
	}

	app, err := NewApp(pwsConf.TZ)

	if err != nil {
		log.Fatal(err)
	}

	client, _ := weathermetrics.NewMQTTClient(mqttConf)

	log.Printf("Connecting to %s", fmt.Sprintf("tcp://%s", mqttConf.MQTTServer))

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	c := make(chan RTL433Message)
	sub(client, mqttConf.Topic, app.weatherPubHandler(c))
	defer MQTTClose(client, mqttConf.Topic)

	timer := time.After(time.Second * 60)

	data := RTL433Message{Data: make(map[string]string)}

	// Wait for interrupt signal to gracefully shutdown the subscriber
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

outerloop:
	for {
		select {
		case msg := <-c:
			data.Timestamp = msg.Timestamp
			for key := range msg.Data {
				data.Data[key] = msg.Data[key]
			}

		case <-timer:
			d := time.Since(*data.Timestamp)

			if d.Minutes() > 5 {
				log.Printf("timestamp is more than 5 minutes out of date: %v",
					*data.Timestamp,
				)
				continue outerloop
			}

			resp, err := submitMeasurement(*id, *key, data.Data)

			if err != nil {
				log.Print(err)
				continue outerloop
			}

			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			log.Printf("%d %s", resp.StatusCode, body)
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
		if k == "timestamp" {
			continue
		}
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

func MQTTClose(client mqtt.Client, topic string) {
	client.Unsubscribe(topic)
	client.Disconnect(250)
}
