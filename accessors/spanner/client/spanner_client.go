package client

import (
	"context"
	"sync"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
)

var once sync.Once

var spannerClient *sp.Client

func GetInstance(ctx context.Context, dbURI string) *sp.Client {
	if spannerClient == nil {
		once.Do(func() {
			spannerClient, _ = sp.NewClient(ctx, dbURI)
		})
		return spannerClient
	}
	return spannerClient
}

// ValidateTables validates that all the tables in the database are empty.
// It returns the name of the first non-empty table if found, and an empty string otherwise.
func ValidateTables(ctx context.Context, spannerClient *sp.Client, spDialect string) (string, error) {
	infoSchema := spanner.InfoSchemaImpl{Client: spannerClient, Ctx: ctx, SpDialect: spDialect}
	tables, err := infoSchema.GetTables()
	if err != nil {
		return "", err
	}
	for _, table := range tables {
		count, err := infoSchema.GetRowCount(table)
		if err != nil {
			return "", err
		}
		if count != 0 {
			return table.Name, nil
		}
	}
	return "", nil
}