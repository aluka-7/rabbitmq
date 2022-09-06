package main

import (
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/configuration/backends"
	"github.com/aluka-7/rabbitmq"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"

	"os"
	"os/signal"
	"syscall"
)

func main() {
	conf := configuration.MockEngine(nil, backends.StoreConfig{Exp: map[string]string{
		"/fc/base/esb/client": "{\"enabled\":true}",
		"/fc/base/esb/9999":   "{\"username\":\"guest\",\"password\":\"guest\",\"brokerURL\":\"localhost:5672\"}",
	}})
	client, _ := rabbitmq.Engine(conf, "9999").Open()
	if consumer, err := client.Consumer("sys_esb"); err == nil {
		defer client.Close() // 关闭 client 会清理所有相关 producer & consumer
		log.Info().Msg("fanout-msg receiver listening...")
		// 生产者服务的 systemId

		consumer.SetExchangeBinds([]*rabbitmq.ExchangeBinds{
			{
				Exch: rabbitmq.DefaultExchange("sys_esb_8888", amqp.ExchangeFanout, nil),
				Bindings: []*rabbitmq.Binding{
					{
						Queues: []*rabbitmq.Queue{
							// 一个消费者对应一个队列
							rabbitmq.DefaultQueue("fanout_8888", nil),
						},
					},
				},
			},
		})
		if err := consumer.Open(); err == nil {
			msgC := make(chan rabbitmq.Delivery, 1)
			consumer.SetMsgCallback(msgC)
			consumer.SetQos(10)
			go func() {
				for msg := range msgC {
					log.Info().Msgf("Tag(%d) Body: %s\n", msg.DeliveryTag, string(msg.Body))
					msg.Ack(true)
				}
				log.Info().Msg("go routine exit.")
			}()
			defer close(msgC)
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
			for {
				s := <-c
				log.Info().Msgf("fanout-msg receiver receive a signal: %s", s.String())
				switch s {
				case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
					log.Info().Msg("fanout-msg receiver exit")
					return
				default:
					return
				}
			}
		} else {
			log.Err(err).Msg("init Consumer has error")
		}
	} else {
		log.Err(err).Msg("init client has error")
	}
}
