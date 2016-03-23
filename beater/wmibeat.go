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

	"github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
	"github.com/eskibars/wmibeat/config"
)

type Wmibeat struct {
	beatConfig   *config.Config
	done         chan struct{}
	period       time.Duration
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

	return nil
}

func (bt *Wmibeat) Run(b *beat.Beat) error {
	logp.Info("wmibeat is running! Hit CTRL-C to stop it.")
	
	ticker := time.NewTicker(bt.period)	
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
		
		var allValues []common.MapStr
		for _, class := range bt.beatConfig.Wmibeat.Classes {
			if len(class.Fields) > 0 {
				ole.CoInitialize(0)
				wmiscriptObj, err := oleutil.CreateObject("WbemScripting.SWbemLocator")
				if err != nil {
					return err
				}
				wmiqi, err := wmiscriptObj.QueryInterface(ole.IID_IDispatch)
				if err != nil {
					return err
				}
				defer wmiscriptObj.Release()
				serviceObj, err := oleutil.CallMethod(wmiqi, "ConnectServer")
				if err != nil {
					return err
				}
				defer wmiqi.Release()
				service := serviceObj.ToIDispatch()
				defer serviceObj.Clear()
				
				var query bytes.Buffer
				wmiFields := class.Fields
				query.WriteString("SELECT ")
				query.WriteString(strings.Join(wmiFields, ","))
				query.WriteString(" FROM ")
				query.WriteString(class.Class)
				if class.WhereClause != "" {
					query.WriteString(" WHERE ")
					query.WriteString(class.WhereClause)
				}
				logp.Info("Query: " + query.String())
				resultObj, err := oleutil.CallMethod(service, "ExecQuery", query.String())
				if err != nil {
					return err
				}
				result := resultObj.ToIDispatch()
				defer resultObj.Clear()
				countObj, err := oleutil.GetProperty(result, "Count")
				if err != nil {
					return err
				}
				count := int(countObj.Val)
				defer countObj.Clear()
				
				var classValues []common.MapStr
				for i :=0; i < count; i++ {
					rowObj, err := oleutil.CallMethod(result, "ItemIndex", i)
					if err != nil {
						return err
					}
					row := rowObj.ToIDispatch()
					defer rowObj.Clear()
					var rowValues common.MapStr
					for _, j := range wmiFields {
						wmiObj, err := oleutil.GetProperty(row, j)
						
						if err != nil {
							return err
						}
						switch wmiObj.Value().(type) {
							case string:
								rowValues = common.MapStrUnion(rowValues, common.MapStr { j: wmiObj.ToString() } )
						}
						defer wmiObj.Clear()
						
					}
					classValues = append(classValues, rowValues)
					rowValues = nil
				}
				allValues = append(allValues, classValues...)
				ole.CoUninitialize()
				classValues = nil
				
			} else {
				var errorString bytes.Buffer
				errorString.WriteString("No fields defined for class ")
				errorString.WriteString(class.Class)
				errorString.WriteString(".  Skipping")
				logp.Warn(errorString.String())
			}
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
	return nil
}

func (bt *Wmibeat) Stop() {
	close(bt.done)
}
