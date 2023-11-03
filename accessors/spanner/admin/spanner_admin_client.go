package admin

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
)

var once sync.Once

var spannerAdminClient *database.DatabaseAdminClient

func GetInstance(ctx context.Context) *database.DatabaseAdminClient {
	if spannerAdminClient == nil {
		once.Do(func() {
			spannerAdminClient, _ = database.NewDatabaseAdminClient(ctx)
		})
		return spannerAdminClient
	}
	return spannerAdminClient
}

// CheckExistingDb checks whether the database with dbURI exists or not.
// If API call doesn't respond then user is informed after every 5 minutes on command line.
func CheckExistingDb(ctx context.Context, spannerAdminClient *database.DatabaseAdminClient, dbURI string) (bool, error) {
	gotResponse := make(chan bool)
	var err error
	go func() {
		_, err = spannerAdminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbURI})
		gotResponse <- true
	}()
	for {
		select {
		case <-time.After(5 * time.Minute):
			fmt.Println("WARNING! API call not responding: make sure that spanner api endpoint is configured properly")
		case <-gotResponse:
			if err != nil {
				if utils.ContainsAny(strings.ToLower(err.Error()), []string{"database not found"}) {
					return false, nil
				}
				return false, fmt.Errorf("can't get database info: %s", err)
			}
			return true, nil
		}
	}
}