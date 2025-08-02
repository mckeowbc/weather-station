package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/url"
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

var weatherPubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received weather message: %s from topic: %s\n", msg.Payload(), msg.Topic())
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

func main() {
	temp := flag.Int("temp", 0, "Temperature")
	humidity := flag.Int("humidity", 0, "Humdity")
	flag.Parse()
	var conf Config
	if err := envconfig.Process("test-client", &conf); err != nil {
		log.Fatal(err)
	}

	if len(conf.Username) > 0 && len(conf.Password) == 0 ||
		len(conf.Username) == 0 && len(conf.Password) > 0 {
		log.Fatal("Error: Must specify both username and password")
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", conf.MQTTServer))
	opts.SetClientID("test-client")
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

	for {
		log.Printf("Publishing to weather topic")

		client.Publish("weather", 0, false,
			fmt.Sprintf("{ \"temp\": %d, \"humidity\": %d }", *temp, *humidity))
		time.Sleep(time.Second * 1)
	}
	client.Disconnect(250)
}
