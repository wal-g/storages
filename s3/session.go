package s3

import (
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const DefaultPort = "443"
const HTTP = "http"

// TODO : unit tests
// Given an S3 bucket name, attempt to determine its region
func findBucketRegion(bucket string, config *aws.Config) (string, error) {
	input := s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		return "", err
	}

	output, err := s3.New(sess).GetBucketLocation(&input)
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		// buckets in "US Standard", a.k.a. us-east-1, are returned as a nil region
		return "us-east-1", nil
	}
	// all other regions are strings
	return *output.LocationConstraint, nil
}

// TODO : unit tests
func getAWSRegion(s3Bucket string, config *aws.Config, settings map[string]string) (string, error) {
	if region, ok := settings[RegionSetting]; ok {
		return region, nil
	}
	if config.Endpoint == nil ||
		*config.Endpoint == "" ||
		strings.HasSuffix(*config.Endpoint, ".amazonaws.com") {
		region, err := findBucketRegion(s3Bucket, config)
		return region, errors.Wrapf(err, "%s is not set and s3:GetBucketLocation failed", RegionSetting)
	} else {
		// For S3 compatible services like Minio, Ceph etc. use `us-east-1` as region
		// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
		return "us-east-1", nil
	}
}

func setupReqProxy(endpointSource, port string) *string {
	resp, err := http.Get(endpointSource)
	if err != nil {
		tracelog.ErrorLogger.Printf("Endpoint source error: %v ", err)
		return nil
	}
	if resp.StatusCode != 200 {
		tracelog.ErrorLogger.Printf("Endpoint source bad status code: %v ", resp.StatusCode)
		return nil
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err == nil {
		return aws.String(net.JoinHostPort(string(bytes), port))
	}
	tracelog.ErrorLogger.Println("Endpoint source reading error:", err)
	return nil
}

func configWithSettings(config *aws.Config, bucket string, settings map[string]string) (*aws.Config, error) {
	// DefaultRetryer implements basic retry logic using exponential backoff for
	// most services. If you want to implement custom retry logic, you can implement the
	// request.Retryer interface.
	config = request.WithRetryer(config, client.DefaultRetryer{NumMaxRetries: MaxRetries})

	if endpoint, ok := settings[EndpointSetting]; ok {
		config = config.WithEndpoint(endpoint)
	}

	if s3ForcePathStyleStr, ok := settings[ForcePathStyleSetting]; ok {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s", ForcePathStyleSetting)
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region, err := getAWSRegion(bucket, config, settings)
	if err != nil {
		return nil, err
	}
	config = config.WithRegion(region)

	return config, nil
}

// TODO : unit tests
func createSession(bucket string, settings map[string]string) (*session.Session, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	c, err := configWithSettings(s.Config, bucket, settings)
	if err != nil {
		return nil, err
	}
	s.Config = c

	filePath := settings[s3CertFile]
	if filePath != "" {
		if file, err := os.Open(filePath); err == nil {
			defer file.Close()
			s, err := session.NewSessionWithOptions(session.Options{Config: *s.Config, CustomCABundle: file})
			return s, err
		} else {
			return nil, err
		}
	}

	if endpointSource, ok := settings[EndpointSourceSetting]; ok {
		s.Handlers.Validate.PushBack(func(request *request.Request) {
			src := setupReqProxy(endpointSource, getEndpointPort(settings))
			if src != nil {
				tracelog.DebugLogger.Printf("using endpoint %s", *src)
				host := strings.TrimPrefix(*s.Config.Endpoint, "https://")
				request.HTTPRequest.Host = host
				request.HTTPRequest.Header.Add("Host", host)
				request.HTTPRequest.URL.Host = *src
				request.HTTPRequest.URL.Scheme = HTTP
			} else {
				tracelog.DebugLogger.Printf("using endpoint %s", *s.Config.Endpoint)
			}
		})
	}
	return s, err
}

func getEndpointPort(settings map[string]string) string {
	if port, ok := settings[EndpointPortSetting]; ok {
		return port
	}
	return DefaultPort
}
