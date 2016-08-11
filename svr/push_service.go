package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

type PushService interface {
	Push(message string, notifyCount int, data interface{}, tokens []string) error
}

var ErrInvalidApiStatus = errors.New("invalid status code")

const osEndpoint = "https://onesignal.com/api/v1/"

type pnService struct {
	appId string
}

func NewPushService(appId string) PushService {
	return &pnService{
		appId: appId,
	}
}

func (p *pnService) pwCall(method string, data []byte) error {
	url := osEndpoint + method
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	request.Header.Set("Content-Type", "application/json")

	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == 200 {
		return nil
	}
	return ErrInvalidApiStatus
}

func (p *pnService) Push(msg string, notifyCount int, data interface{}, tokens []string) error {
	requestData := map[string]interface{}{
		"app_id":             p.appId,
		"data":               data,
		"contents":           map[string]interface{}{"en": msg},
		"include_ios_tokens": tokens,
	}

	jsonRequest, err := json.Marshal(requestData)
	if err != nil {
		return err
	}

	err = p.pwCall("notifications", jsonRequest)
	if err != nil {
		return err
	}
	return nil
}
