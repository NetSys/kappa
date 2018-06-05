package executor

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func Rename(tbucket string, tkey string, bucket string, key string) error {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return err
	}

	svc := s3.New(sess)
	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &key,
		CopySource: aws.String(fmt.Sprintf("%s/%s", tbucket, tkey)),
	})
	if err != nil {
		return err
	}

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &tbucket,
		Key:    &tkey,
	})
	return err
}
