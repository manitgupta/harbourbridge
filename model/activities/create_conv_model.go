package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

type CreateConvActivityInput struct {
	Conv *internal.Conv
}

type CreateConvActivityOutput struct {
	Conv *internal.Conv
}
