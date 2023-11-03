package activities

import (
	"context"
	"fmt"

	datastreamAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	pubsubAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/pubsub"
	storageAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils"
)

func SetupPubSubActivity(ctx context.Context, input activityModel.SetupPubSubActivityInput) (activityModel.SetupPubSubActivityOutput, error) {
	pubsubClient := pubsubAccessor.GetInstance(ctx, input.GcpProjectId)
	pubsubCfg, err := pubsubAccessor.CreatePubsubTopicAndSubscription(ctx, pubsubClient, input.SourceDbName)
	if err != nil {
		fmt.Printf("Could not create pubsub topic and subscription")
		return activityModel.SetupPubSubActivityOutput{}, err
	}
	datastreamClient := datastreamAccessor.GetInstance(ctx)
	gcsProfile, err := datastreamAccessor.GetGCSPathFromConnectionProfile(ctx, datastreamClient, input.GcpProjectId, input.GcsConnectionProfile)
	if err != nil {
		fmt.Printf("Could not fetch GCS path from datastream")
		return activityModel.SetupPubSubActivityOutput{}, err
	}
	bucketName := gcsProfile.Bucket
	prefix := gcsProfile.RootPath + input.GcsConnectionProfile.Prefix
	prefix = utils.ConcatDirectoryPath(prefix, "data/")
	if err != nil {
		fmt.Printf("unable to fetch target bucket from datastream")
		return activityModel.SetupPubSubActivityOutput{}, err
	}

	storageClient := storageAccessor.GetInstance(ctx)
	notificationID, err := storageAccessor.CreateNotificationOnBucket(ctx, storageClient, input.GcpProjectId, pubsubCfg.TopicId, bucketName, prefix)
	if err != nil {
		fmt.Printf("Could not create pubsub resources. Some permissions missing. Please check https://googlecloudplatform.github.io/spanner-migration-tool/permissions.html for required pubsub permissions. error=%v", err)
		return activityModel.SetupPubSubActivityOutput{}, err
	}
	pubsubCfg.BucketName = bucketName
	pubsubCfg.NotificationId = notificationID
	fmt.Printf("Successfully created pubsub topic id=%s, subscription id=%s, notification for bucket=%s with id=%s.\n", pubsubCfg.TopicId, pubsubCfg.SubscriptionId, bucketName, notificationID)
	return activityModel.SetupPubSubActivityOutput{PubsubCfg: pubsubCfg}, nil
}
