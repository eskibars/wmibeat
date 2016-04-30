// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Wmibeat WmibeatConfig
}

type WmibeatConfig struct {
	Period   string `yaml:"period"`
	Classes  []ClassConfig
}

type ClassConfig struct {
	Class       string    `config:"class"`
	Fields      []string  `config:"fields"`
	WhereClause string    `config:"whereclause"`
	ObjectTitle string    `config:"objecttitlecolumn"`
}
