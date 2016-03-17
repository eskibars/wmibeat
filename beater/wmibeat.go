package beater

import (
	"fmt"
	"time"
	"strings"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/eskibars/wmibeat/config"
)

type Wmibeat struct {
	beatConfig *config.Config
	done       chan struct{}
	period     time.Duration
}

// Creates beater
func New() *Wmibeat {
	return &Wmibeat{
		done: make(chan struct{}),
	}
}

/// *** Beater interface methods ***///

func (bt *Wmibeat) Config(b *beat.Beat) error {

	// Load beater beatConfig
	err := cfgfile.Read(&bt.beatConfig, "")
	if err != nil {
		return fmt.Errorf("Error reading config file: %v", err)
	}

	return nil
}

func (bt *Wmibeat) Setup(b *beat.Beat) error {

	// Setting default period if not set
	if bt.beatConfig.Wmibeat.Period == "" {
		bt.beatConfig.Wmibeat.Period = "1s"
	}

	var err error
	bt.period, err = time.ParseDuration(bt.beatConfig.Wmibeat.Period)
	if err != nil {
		return err
	}
	
	ole.CoInitialize(0)
	wmiscriptobj, _ := oleutil.CreateObject("WbemScripting.SWbemLocator")
	wmiqi, _ := wmiscriptobj.QueryInterface(ole.IID_IDispatch)
	service, _ := oleutil.CallMethod(wmiqi, "ConnectServer").ToIDispatch()

	return nil
}

func (bt *Wmibeat) Run(b *beat.Beat) error {
	logp.Info("wmibeat is running! Hit CTRL-C to stop it.")

	wmi, _ := unknown.QueryInterface(ole.IID_IDispatch)
	ticker := time.NewTicker(bt.period)
	
	
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
		
		var allValues []common.MapStr
		for _, class := range b.beatConfig.Wmibeat.Classes {
			var query = bytes.Buffer
			var wmiFields = b.beatConfig.Wmibeat.Classes[class].fields
			buffer.WriteString("SELECT ")
			buffer.WriteString(trings.Join(wmiFields, ","))
			buffer.WriteString(" FROM ")
			buffer.WriteString(class)
			if b.beatConfig.Wmibeat.Classes[class].whereClause != nil {
				buffer.WriteString(" WHERE ")
				buffer.WriteString(b.beatConfig.Wmibeat.Classes[class].whereClause)
			}
			result, _ := oleutil.CallMethod(service, "ExecQuery", buffer.String()).ToIDispatch()
			count, _ := oleutil.GetProperty(result, "Count")
			
			var classValues []common.MapStr
			for i :=0; i < int(count.Val); i++ {
				row, _ := oleutil.CallMethod(result, "ItemIndex", i).ToIDispatch()
				var rowValues []common.MapStr
				for _, j := range wmiFields {
					wmiValue, _ := oleutil.GetProperty(row, j)
					rowValues = append(rowValues, common.MapStr {j : wmiValue })
				}
				row.Release()
				classValues = append(classValues, rowValues)
				rowValues = nil
			}
			allValues = append(allValues, classValues)
			classValues = nil
		}

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       b.Name,
			"wmi":    allValues,
		}
		b.Events.PublishEvent(event)
		logp.Info("Event sent")
	}
}

func (bt *Wmibeat) Cleanup(b *beat.Beat) error {
	service.Release()
	wmiqi.Release()
	wmiscriptobj.Release()
	ole.CoUninitialize()
	return nil
}

func (bt *Wmibeat) Stop() {
	close(bt.done)
}
