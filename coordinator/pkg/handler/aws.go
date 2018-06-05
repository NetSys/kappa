package handler

import (
	"io"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/yaml.v2"

	cp "github.com/NetSys/kappa/coordinator/pkg/cloudplatform"
	ap "github.com/NetSys/kappa/coordinator/pkg/cloudplatform/aws"
	"github.com/NetSys/kappa/coordinator/pkg/cloudplatform/local"
	"github.com/NetSys/kappa/coordinator/pkg/util"
)

// awsHandler exposes a handler that runs on AWS Lambda.
type awsHandler struct {
	common
	tempChkBucket string // If not an empty string, the bucket is deleted at cleanup time.
}

// createAWS creates an Kappa handler that runs lambdas on AWS Lambda.
// logWriter.Write MAY be called concurrently; the Writer is responsible for preventing undesired interleaving.
func createAWS(conf io.Reader, name string, deployedFiles []string, timeoutSecs int, env EnvT, logWriter io.Writer) (
	*awsHandler, error) {

	var config struct {
		CheckpointBucket string `yaml:"checkpoint_bucket"`
	}

	if conf != nil {
		b, err := ioutil.ReadAll(conf)
		if err != nil {
			return nil, err
		}
		if err = yaml.UnmarshalStrict(b, &config); err != nil {
			return nil, err
		}
	}

	var err error
	chkBucket, tempChkBucket := config.CheckpointBucket, ""
	if chkBucket == "" { // Make a temporary checkpoint bucket.
		tempChkBucket, err = createCheckpointBucket()
		// Don't forget to delete the temporary bucket if a subsequent error occurs in this function!
		if err != nil {
			cleanUpBucket(tempChkBucket)
			return nil, err
		}
		log.Println("handler.createAWS: temporary checkpoint S3 bucket created:", tempChkBucket)
		chkBucket = tempChkBucket
	} else {
		log.Println("handler.createAWS: using checkpoint S3 bucket:", chkBucket)
	}

	penv := cp.EnvT{
		"PLATFORM":          "aws",
		"CHECKPOINT_BUCKET": chkBucket,
	}
	if err = mergeEnv(penv, env); err != nil {
		return nil, err
	}

	aph, err := ap.CreateHandler(name, deployedFiles, penv, timeoutSecs, logWriter)
	if err != nil {
		cleanUpBucket(tempChkBucket)
		return nil, err
	}

	kappaDir, err := detectKappaDir()
	if err != nil {
		return nil, err
	}
	lh, err := local.CreateHandler(kappaDir, name, deployedFiles, penv, 0, logWriter)
	if err != nil {
		return nil, err
	}

	h := &awsHandler{
		common:        common{map[InvokeTarget]cp.Handler{OnCoordinator: lh, OnLambda: aph}},
		tempChkBucket: tempChkBucket,
	}
	return h, nil
}

func (ah *awsHandler) Finalize() {
	ah.finalizePlatform()

	// TODO(zhangwen): deletion might fail if an unfinished lambda process is still putting to the bucket.
	cleanUpBucket(ah.tempChkBucket)
}

// createCheckpointBucket creates an S3 bucket for storing checkpoint files and blocks until the bucket appears.
// If a non-empty name is returned, the bucket has been created successfully (regardless of whether an error is
// returned); the caller may be responsible for deleting the bucket.
func createCheckpointBucket() (name string, err error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return "", err
	}

	svc := s3.New(sess)
	name = "chk-" + util.MakeRandomASCII128()
	_, err = svc.CreateBucket(&s3.CreateBucketInput{Bucket: &name})
	if err != nil {
		return "", err
	}

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &name})
	return name, err
}

// Deletes an S3 bucket.  Is no-op if bucket is an empty string.  Any error encountered is logged and ignored.
func cleanUpBucket(bucket string) {
	if bucket == "" {
		return
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Printf("cleanUpBucket: session creation failed: %v", err)
		return
	}
	svc := s3.New(sess)

	// Delete all items in the bucket first, since S3 only allows deleting empty buckets.
	// Adapted from: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/s3-example-basic-bucket-operations.html.
	for hasMoreObjects := true; hasMoreObjects; {
		resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: &bucket})
		if err != nil {
			log.Println("cleanUpBucket: ListObjects failed:", err)
			return
		}
		numObjs := len(resp.Contents)
		if numObjs == 0 {
			break
		}

		var objs = make([]*s3.ObjectIdentifier, numObjs)
		for i, o := range resp.Contents {
			objs[i] = &s3.ObjectIdentifier{Key: o.Key}
		}
		items := s3.Delete{Objects: objs}

		_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{Bucket: &bucket, Delete: &items})
		if err != nil {
			log.Println("cleanUpBucket: DeleteObjects failed:", err)
			return
		}

		log.Printf("cleanUpBucket: %d items deleted from S3 bucket: %s", numObjs, bucket)
		hasMoreObjects = *resp.IsTruncated
	}

	// Hopefully the bucket remains empty.  Delete the bucket now.
	_, err = svc.DeleteBucket(&s3.DeleteBucketInput{Bucket: &bucket})
	if err != nil {
		log.Println("cleanUpBucket: DeleteBucket failed:", err)
		return
	}

	err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{Bucket: &bucket})
	if err != nil {
		log.Println("cleanUpBucket: WaitUntilBucketNotExists failed:", err)
		return
	}

	log.Println("cleanUpBucket: S3 bucket successfully deleted:", bucket)
}
