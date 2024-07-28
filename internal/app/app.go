package app

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/core-go/activemq"
	"github.com/core-go/health"
	ah "github.com/core-go/health/activemq"
	hm "github.com/core-go/health/mongo"
	w "github.com/core-go/mongo/writer"
	"github.com/core-go/mq"
	v "github.com/core-go/mq/validator"
	"github.com/core-go/mq/zap"
	"github.com/go-stomp/stomp/v3"
)

type ApplicationContext struct {
	HealthHandler *health.Handler
	Subscribe     func(ctx context.Context, handle func(context.Context, []byte, map[string]string))
	Handle        func(context.Context, []byte, map[string]string)
}

func NewApp(ctx context.Context, cfg Config) (*ApplicationContext, error) {
	log.Initialize(cfg.Log)
	client, er1 := mongo.Connect(ctx, options.Client().ApplyURI(cfg.Mongo.Uri))
	if er1 != nil {
		log.Error(ctx, "Cannot connect to MongoDB: Error: "+er1.Error())
		return nil, er1
	}
	db := client.Database(cfg.Mongo.Database)

	logError := log.ErrorMsg
	var logInfo func(context.Context, string)
	if log.IsInfoEnable() {
		logInfo = log.InfoMsg
	}

	subscriber, er2 := activemq.NewSubscriberByConfig(cfg.Amq, stomp.AckAuto, logError, true)
	if er2 != nil {
		log.Error(ctx, "Cannot create a new subscriber. Error: "+er2.Error())
		return nil, er2
	}
	validator, er3 := v.NewValidator[*User]()
	if er3 != nil {
		return nil, er3
	}
	errorHandler := mq.NewErrorHandler[*User](logError)
	sender, er4 := activemq.NewSenderByConfig(cfg.Amq, "")
	if er4 != nil {
		return nil, er4
	}
	writer := w.NewWriter[*User](db, "user")
	handler := mq.NewRetryHandlerByConfig[User](cfg.Retry, writer.Write, validator.Validate, errorHandler.RejectWithMap, nil, sender.SendWithFrame, logError, logInfo)
	mongoChecker := hm.NewHealthChecker(client)
	subscriberChecker := ah.NewHealthChecker(cfg.Amq.Addr, "amq_subscriber")
	healthHandler := health.NewHandler(mongoChecker, subscriberChecker)

	return &ApplicationContext{
		HealthHandler: healthHandler,
		Subscribe:     subscriber.Subscribe,
		Handle:        handler.Handle,
	}, nil
}
