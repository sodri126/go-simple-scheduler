package scheduler

import (
	"encoding/json"
	"time"
)

func NewJsonResponse() ListConverter {
	return &jsonResponse{}
}

type jsonResponse struct {
}

type jsonData struct {
	Key      string    `json:"key"`
	DateTime time.Time `json:"date_time"`
}

func (d *jsonResponse) transformToJsonData(responses []*ResponseScheduler) (data []*jsonData) {
	for i := 0; i < len(responses); i++ {
		data = append(data, &jsonData{
			Key:      responses[i].Key,
			DateTime: responses[i].Time,
		})
	}
	return
}

func (d *jsonResponse) Convert(data []*ResponseScheduler) ([]byte, error) {
	transformData := d.transformToJsonData(data)
	return json.Marshal(transformData)
}
