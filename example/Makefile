run_fan_out_sender: ## 运行 fan-out 模式示例发送方
	@go run fan-out/sender.go

run_fan_out_receiver: ## 运行 fan-out 模式示例接收方
	@go run fan-out/receiver.go

.DEFAULT_GOAL :=
help: ## show help info
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

