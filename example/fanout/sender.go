package main

import (
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/configuration/backends"
	"github.com/aluka-7/rabbitmq"
	"github.com/rs/zerolog/log"
)

func main() {
	conf := configuration.MockEngine(nil, backends.StoreConfig{Exp: map[string]string{
		"/system/base/amq/9999": "{\"username\":\"guest\",\"password\":\"guest\",\"brokerURL\":\"localhost:5672\"}",
	}})
	ex := "sys_amq_8888"
	client, _ := rabbitmq.Engine(conf, "9999").Open()
	if producer, err := client.Producer(ex); err == nil {
		defer client.Close() // 关闭 client 会清理所有相关 producer & consumer
		if err := producer.ForFanout(ex, "{\"hello\": \"world\"}"); err == nil {
			log.Info().Msg("fanout-msg sender finish")
		} else {
			log.Err(err).Msg("fanout-msg sender has error")
		}
	} else {

		log.Err(err).Msg("init client has error")
	}
}
