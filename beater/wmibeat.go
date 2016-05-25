package beater

import (
	"fmt"
	"time"
	"strings"
	"bytes"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
	"github.com/eskibars/wmibeat/config"
)

type Wmibeat struct {
	beatConfig           *config.Config
	events               publisher.Client
	compiledWmiQueries   map[string]string
	done                 chan struct{}
	period               time.Duration
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
	bt.events = b.Publisher.Connect()

	// Setting default period if not set
	if bt.beatConfig.Wmibeat.Period == "" {
		bt.beatConfig.Wmibeat.Period = "1s"
	}

	var err error
	bt.period, err = time.ParseDuration(bt.beatConfig.Wmibeat.Period)
	if err != nil {
		return err
	}

	bt.compiledWmiQueries = map[string]string{}
	for _, class := range bt.beatConfig.Wmibeat.Classes {
		if len(class.Fields) == 0 {
			var errorString bytes.Buffer
			errorString.WriteString("No fields defined for class ")
			errorString.WriteString(class.Class)
			errorString.WriteString(".  Skipping")
			logp.Warn(errorString.String())
			continue
		}
		var query bytes.Buffer
		query.WriteString("SELECT ")
		query.WriteString(strings.Join(class.Fields, ","))
		query.WriteString(" FROM ")
		query.WriteString(class.Class)
		if class.WhereClause != "" {
			query.WriteString(" WHERE ")
			query.WriteString(class.WhereClause)
		}
		bt.compiledWmiQueries[class.Class] = query.String()
	}

	return nil
}

func (bt *Wmibeat) Run(b *beat.Beat) error {
	var err error

	logp.Info("wmibeat is running! Hit CTRL-C to stop it.")
	
	ticker := time.NewTicker(bt.period)	
	defer ticker.Stop()

	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		err := bt.RunOnce(b)
		if err != nil {
			logp.Err("Unable to run WMI queries: %v", err)
			break
		}
	}

	return err
}

func (bt *Wmibeat) RunOnce(b *beat.Beat) error {
	ole.CoInitializeEx(0, 0)
	defer ole.CoUninitialize()

	wmiscriptObj, err := oleutil.CreateObject("WbemScripting.SWbemLocator")
	if err != nil {
		logp.Err("Unable to create object: %v", err)
		return err
	}
	defer wmiscriptObj.Release()

	wmiqi, err := wmiscriptObj.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		logp.Err("Unable to get locator query interface: %v", err)
		return err
	}
	defer wmiqi.Release()

	serviceObj, err := oleutil.CallMethod(wmiqi, "ConnectServer")
	if err != nil {
		logp.Err("Unable to connect to server: %v", err)
		return err
	}
	defer serviceObj.Clear()

	service := serviceObj.ToIDispatch()

	events := []common.MapStr{}

	for _, class := range bt.beatConfig.Wmibeat.Classes {
		query, exists := bt.compiledWmiQueries[class.Class]
		if !exists { 
			continue
		}

		logp.Info("Query: " + query)

		resultObj, err := oleutil.CallMethod(service, "ExecQuery", query, "WQL")
		if err != nil {
			logp.Err("Unable to execute query: %v", err)
			return err
		}
		defer resultObj.Clear()

		result := resultObj.ToIDispatch()
		countObj, err := oleutil.GetProperty(result, "Count")
		if err != nil {
			logp.Err("Unable to get result count: %v", err)
			return err
		}
		defer countObj.Clear()

		count := int(countObj.Val)

		for i :=0; i < count; i++ {
			rowObj, err := oleutil.CallMethod(result, "ItemIndex", i)
			if err != nil {
				logp.Err("Unable to get result item by index: %v", err)
				return err
			}
			defer rowObj.Clear()

			row := rowObj.ToIDispatch()

			event := common.MapStr{
				"@timestamp": common.Time(time.Now()),
				"type":       b.Name,
				"class":    class.Class,
			}

			for _, fieldName := range class.Fields {
				wmiObj, err := oleutil.GetProperty(row, fieldName)
				if err != nil {
					logp.Err("Unable to get propery by name: %v", err)
					return err
				}
				defer wmiObj.Clear()

				var objValue = wmiObj.Value()
				event[fieldName] = objValue
			}

			events = append(events, event)
		}
	}

	bt.events.PublishEvents(events)

	return err
}

func (bt *Wmibeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (bt *Wmibeat) Stop() {
	close(bt.done)
	bt.events.Close()
}
