package main

import (
	"context"
	"fmt"
	bybit "github.com/wuhewuhe/bybit.go.api"
)

func main() {
	GetFundingRate()
}

func GetFundingRate() {
	client := bybit.NewBybitHttpClient("", "", bybit.WithBaseURL(bybit.TESTNET))
	params := map[string]interface{}{"category": "linear", "symbol": "BTCUSDT"}
	serverResult, err := client.NewUtaBybitServiceWithParams(params).GetFundingRateHistory(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(bybit.PrettyPrint(serverResult))
}