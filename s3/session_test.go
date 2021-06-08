package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"testing"
)

var bucket = "s3://test-bucket/wal-g-test-folder/Sub0"
var settings = map[string]string{
	EndpointSetting: "http://s3.mdst.yandex.net/",
}
var config = defaults.Get().Config.WithRegion(settings[RegionSetting])

func TestGetAWSRegionWithEmptyEndpoint(t *testing.T) {
	findBucketRegion = func(bucket string, config *aws.Config) (string, error) {
		return "europe", nil
	}

	region, err := GetAWSRegion(bucket, config, settings)

	assert.Nil(t, err)
	assert.Equal(t, region, "europe")
}

func TestGetAWSRegionWithPort(t *testing.T) {
	bucket = "s3://test-bucket:8080/wal-g-test-folder/Sub0"
	findBucketRegion = func(bucket string, config *aws.Config) (string, error) {
		return "europe", nil
	}

	region, err := GetAWSRegion(bucket, config, settings)

	assert.Nil(t, err)
	assert.Equal(t, region, "europe")
}

func TestGetAWSRegionWithEndpoint(t *testing.T) {
	config.Endpoint = aws.String("s3://test-bucket:8080/wal-g-test-folder/Sub0")
	findBucketRegion = func(bucket string, config *aws.Config) (string, error) {
		return "europe", nil
	}

	region, err := GetAWSRegion(bucket, config, settings)

	assert.Nil(t, err)
	assert.Equal(t, region, "us-east-1")
}
