package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

var once sync.Once

var pubsubClient *pubsub.Client

func GetInstance(ctx context.Context, projectId string) *pubsub.Client {
	if pubsubClient == nil {
		once.Do(func() {
			pubsubClient, _ = pubsub.NewClient(ctx, projectId)
		})
		return pubsubClient
	}
	return pubsubClient
}

func CreatePubsubTopicAndSubscription(ctx context.Context, pubsubClient *pubsub.Client, dbName string) (internal.PubsubCfg, error) {
	pubsubCfg := internal.PubsubCfg{}
	// Generate ID
	subscriptionId, err := utils.GenerateName("smt-sub-" + dbName)
	if err != nil {
		return pubsubCfg, fmt.Errorf("error generating pubsub subscription ID: %v", err)
	}
	pubsubCfg.SubscriptionId = subscriptionId

	topicId, err := utils.GenerateName("smt-topic-" + dbName)
	if err != nil {
		return pubsubCfg, fmt.Errorf("error generating pubsub topic ID: %v", err)
	}
	pubsubCfg.TopicId = topicId

	// Create Topic and Subscription
	topicObj, err := pubsubClient.CreateTopic(ctx, pubsubCfg.TopicId)
	if err != nil {
		return pubsubCfg, fmt.Errorf("pubsub topic could not be created: %v", err)
	}

	_, err = pubsubClient.CreateSubscription(ctx, pubsubCfg.SubscriptionId, pubsub.SubscriptionConfig{
		Topic:             topicObj,
		AckDeadline:       time.Minute * 10,
		RetentionDuration: time.Hour * 24 * 7,
	})
	if err != nil {
		return pubsubCfg, fmt.Errorf("pubsub subscription could not be created: %v", err)
	}
	return pubsubCfg, nil
}