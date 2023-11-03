package activities

import "github.com/GoogleCloudPlatform/spanner-migration-tool/internal"

type ValidateSchemaActivityInput struct {
	DbURI string
	Conv *internal.Conv
}

type ValidateSchemaActivityOutput struct {
	ValidationResult string
}
