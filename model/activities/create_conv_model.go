package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

type CreateConvActivityInput struct {
	SessionFilePath string
}

type CreateConvActivityOutput struct {
	Conv *internal.Conv
}
