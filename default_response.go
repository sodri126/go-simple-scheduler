package scheduler

import (
	"github.com/jedib0t/go-pretty/v6/table"
)

func NewDefaultResponse() ListConverter {
	return &defaultResponse{
		tableWriter: table.NewWriter(),
	}
}

type defaultResponse struct {
	tableWriter table.Writer
}

func (d *defaultResponse) Convert(data []*ResponseScheduler) (res []byte, err error) {
	d.tableWriter.AppendHeader(table.Row{"No.", "Key", "DateTime Time"})
	for i := 0; i < len(data); i++ {
		d.tableWriter.AppendRow(table.Row{i + 1, data[i].Key, data[i].Time})
	}
	res = []byte(d.tableWriter.Render())
	return
}
