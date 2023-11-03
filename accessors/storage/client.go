package storage

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils/constants"
)

var once sync.Once

var gcsClient *storage.Client

func GetInstance(ctx context.Context) *storage.Client {
	if gcsClient == nil {
		once.Do(func() {
			gcsClient, _ = storage.NewClient(ctx)
		})
		return gcsClient
	}
	return gcsClient
}

func CreateNotificationOnBucket(ctx context.Context, storageClient *storage.Client, projectID, topicID, bucketName, prefix string) (string, error) {
	notification := storage.Notification{
		TopicID:          topicID,
		TopicProjectID:   projectID,
		PayloadFormat:    storage.JSONPayload,
		ObjectNamePrefix: prefix,
	}

	createdNotification, err := storageClient.Bucket(bucketName).AddNotification(ctx, &notification)
	if err != nil {
		return "", fmt.Errorf("GCS Notification could not be created: %v", err)
	}
	return createdNotification.ID, nil
}

func WriteToGCS(ctx context.Context, gcsClient *storage.Client, filePath string, fileName string, data string) (error) {
	u, err := parseGCSFilePath(filePath)
	if err != nil {
		return fmt.Errorf("parseFilePath: unable to parse file path: %v", err)
	}
	bucketName := u.Host
	bucket := gcsClient.Bucket(bucketName)
	obj := bucket.Object(u.Path[1:] + fileName)

	w := obj.NewWriter(ctx)
	if _, err := fmt.Fprint(w, data); err != nil {
		fmt.Printf("Failed to write to Cloud Storage: %s", filePath)
		return err
	}
	if err := w.Close(); err != nil {
		fmt.Printf("Failed to close GCS file: %s", filePath)
		return err
	}
	return nil
}

func parseGCSFilePath(filePath string) (*url.URL, error) {
	if len(filePath) == 0 {
		return nil, fmt.Errorf("found empty GCS path")
	}
	if filePath[len(filePath)-1] != '/' {
		filePath = filePath + "/"
	}
	u, err := url.Parse(filePath)
	if err != nil {
		return nil, fmt.Errorf("parseFilePath: unable to parse file path %s", filePath)
	}
	if u.Scheme != constants.GCS_SCHEME {
		return nil, fmt.Errorf("not a valid GCS path: %s, should start with 'gs'", filePath)
	}
	return u, nil
}