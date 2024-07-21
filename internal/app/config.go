package app

import (
	"github.com/core-go/activemq"
	"github.com/core-go/health/server"
	"github.com/core-go/mq"
	"github.com/core-go/mq/zap"
)

type Config struct {
	Server server.ServerConf     `mapstructure:"server"`
	Log    log.Config            `mapstructure:"log"`
	Mongo  MongoConfig           `mapstructure:"mongo"`
	Retry  mq.RetryHandlerConfig `mapstructure:"retry"`
	Amq    activemq.Config       `mapstructure:"amq"`
}

type MongoConfig struct {
	Uri      string `yaml:"uri" mapstructure:"uri" json:"uri,omitempty" gorm:"column:uri" bson:"uri,omitempty" dynamodbav:"uri,omitempty" firestore:"uri,omitempty"`
	Database string `yaml:"database" mapstructure:"database" json:"database,omitempty" gorm:"column:database" bson:"database,omitempty" dynamodbav:"database,omitempty" firestore:"database,omitempty"`
}