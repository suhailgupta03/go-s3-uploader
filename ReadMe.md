Simple go library for uploading to S3. Enables auto bucket creation.

# Install
```shell
go get -u github.com/suhailgupta03/go-s3-uploader@latest
```
# Example
```go
package main

import (
	"github.com/suhailgupta03/go-s3-uploader"
	"github.com/zerodha/logf"
	"time"
)

func main() {
	lo := logf.New(logf.Opts{
		EnableColor:          true,
		Level:                logf.DebugLevel,
		CallerSkipFrameCount: 3,
		EnableCaller:         true,
		TimestampFormat:      time.RFC3339Nano,
		DefaultFields:        []any{"scope", "example"},
	})
	s := S3Uploader.S3{
		// Optional if you want to point to a specific path
		AWSConfig: &S3Uploader.Config{
			AWSConfigFile:            "/go-s3-uploader/config",
			AWSSharedCredentialsFile: "/go-s3-uploader/credentials",
		},
		BucketName: "test-foo-xyz",
		RetentionConfig: &S3Uploader.RetentionConfig{
			Use:  true,
			Time: time.Now().AddDate(0, 0, 1),
		},
		Lo: &lo,
	}

	testData := "this is some data"
	fileIdentifier := "file-id"
	uploadId, _ := s.UploadFile([]byte(testData), fileIdentifier)
	if uploadId != nil {
		lo.Info("Success!", "upload-id", *uploadId)
	}
}

```