package weathermetrics

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

/*
 * Config
 */
type MQTTConfig struct {
	MQTTServer string `envconfig:"MQTT_SERVER" default:"mqtt:1883"`
	Topic      string `envconfig:"MQTT_TOPIC" default:"rtl_433/+/events"`
	Username   string `envconfig:"MQTT_USERNAME"`
	Password   string `envconfig:"MQTT_PASSWORD"`
	ClientID   string `envconfig:"MQTT_CLIENTID"`
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

func NewMQTTClient(conf MQTTConfig) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", conf.MQTTServer))
	opts.SetClientID(conf.ClientID)
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

	return client, nil
}

/*
 * MQTT Message Handlers
 */
func messagePubHandler(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
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
