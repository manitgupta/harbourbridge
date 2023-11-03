package utils

import (
	"encoding/json"
	"fmt"
	"os"

	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(sessionFilePath string) (conv *internal.Conv, err error) {
	s, err := os.ReadFile(sessionFilePath)
	if err != nil {
		fmt.Printf("Error reading session file: %v\n", err)
		return nil, err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return nil, err
	}
	return conv, nil
}

func ContainsAny(s string, l []string) bool {
	for _, a := range l {
		if strings.Contains(s, a) {
			return true
		}
	}
	return false
}

func ConcatDirectoryPath(basePath, subPath string) string {
	// ensure basPath doesn't start with '/' and ends with '/'
	if basePath == "" || basePath == "/" {
		basePath = ""
	} else {
		if basePath[0] == '/' {
			basePath = basePath[1:]
		}
		if basePath[len(basePath)-1] != '/' {
			basePath = basePath + "/"
		}
	}
	// ensure subPath doesn't start with '/' ends with '/'
	if subPath == "" || subPath == "/" {
		subPath = ""
	} else {
		if subPath[0] == '/' {
			subPath = subPath[1:]
		}
		if subPath[len(subPath)-1] != '/' {
			subPath = subPath + "/"
		}
	}
	path := fmt.Sprintf("%s%s", basePath, subPath)
	return path
}