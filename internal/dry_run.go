// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package internal

var enabled = false

// DryRun returns true if dry run mode is enabled.
func DryRun() bool {
	return enabled
}

// DryRunInit determines whether dry run mode is enabled.
// Generally there should be one call to DryRunInit at startup.
func DryRunInit(b bool) {
	enabled = b
}
