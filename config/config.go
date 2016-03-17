// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import (
	"github.com/urso/ucfg"
)

type Config struct {
	Wmibeat WmibeatConfig
}

type WmibeatConfig struct {
	Period string `yaml:"period"`
	WMIQueries []*ucfg.Config
}
