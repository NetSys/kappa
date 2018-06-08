package aws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/mholt/archiver"

	cp "github.com/NetSys/kappa/coordinator/pkg/cloudplatform"
	"github.com/NetSys/kappa/coordinator/pkg/util"
)

type handler struct {
	sess         *session.Session
	svc          *lambda.Lambda
	workloadName string
	functionName string // Name of the AWS lambda function.
	timeoutSecs  int

	shouldLog bool
	logWriter io.Writer

	numInvocations uint64 // Total number of AWS Lambda invocations performed.
}

// IAM-related constants.
const (
	iamLoggingRoleName   = "kappa-full"
	iamNoLoggingRoleName = "kappa-no-log"

	iamRoleDescription = "Role for lambdas running Kappa handlers"
	iamTrustPolicy     = `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": {
					"Service": "lambda.amazonaws.com"
				},
				"Action": "sts:AssumeRole"
			}
		]
	}`

	s3ARN      = "arn:aws:iam::aws:policy/AmazonS3FullAccess"       // TODO(zhangwen): limit access to relevant buckets only.
	loggingARN = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess" // TODO(zhangwen): limit access to relevant log group only.
)

// Lambda-related constants.
const (
	handlerFunction = "rt_handler"
	lambdaRuntime   = "python3.6"
	memorySizeMB    = 3008 // Maximum memory allocation on AWS Lambda.
	funcNamePrefix  = "__ir"
	logGroupPrefix  = "/aws/lambda/"
)

// CreateHandler returns a handler representing an AWS lambda function.
// The underlying lambda function is deleted at cleanup time.
// The log is written in the end at finalization time.
func CreateHandler(name string, deployedFiles []string, env cp.EnvT, timeoutSecs int, logWriter io.Writer) (*handler, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		return nil, err
	}

	shouldLog := logWriter != ioutil.Discard // Optimization: don't bother to log if writer always discards.
	role, err := getIAMRole(sess, shouldLog)
	if err != nil {
		return nil, err
	}

	deployedFiles = util.ParseFilterPathByPlatform("aws", deployedFiles)
	zip, handlerModule, err := zipPackage(deployedFiles)
	if err != nil {
		return nil, err
	}
	log.Println("aws.CreateHandler: package zip created")

	env["WHERE"] = "aws-lambda"

	svcLambda := lambda.New(sess)
	l, err := createLambdaFunction(svcLambda, name, zip, env, handlerModule, timeoutSecs, role)
	if err != nil {
		return nil, err
	}
	log.Println("aws.CreateHandler: lambda function created:", *l.FunctionName)

	h := &handler{
		sess:         sess,
		svc:          svcLambda,
		functionName: *l.FunctionName,
		workloadName: name,
		timeoutSecs:  timeoutSecs,

		shouldLog: shouldLog,
		logWriter: logWriter,
	}
	return h, nil
}

func (h *handler) Name() string {
	return h.workloadName
}

func (h *handler) TimeoutSecs() int {
	return h.timeoutSecs
}

// unhandledErrorMessage is AWS Lambda's format for reporting unhandled errors.
type unhandledErrorMessage struct {
	ErrorMessage string `json:"errorMessage"`
}

func (h *handler) Invoke(p []byte) ([]byte, error) {
	input := &lambda.InvokeInput{
		FunctionName:   aws.String(h.functionName),
		InvocationType: aws.String("RequestResponse"), // Run lambda synchronously.
		LogType:        aws.String("Tail"),
		Payload:        p,
	}

	var result *lambda.InvokeOutput
	retryTimeout := time.Millisecond * time.Duration(500+rand.Intn(1000))
	for {
		var err error
		if result, err = h.svc.Invoke(input); err == nil {
			break
		}

		// An error has occurred.
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == lambda.ErrCodeServiceException || aerr.Code() == lambda.ErrCodeTooManyRequestsException {
				log.Printf("aws.Invoke: %s, %s; retrying after %v...", aerr.Code(), aerr.Message(), retryTimeout)
				time.Sleep(retryTimeout)
				retryTimeout *= 2
				continue
			}
		}

		// Abort.
		return nil, fmt.Errorf("aws.Invoke: %v", err)
	}

	atomic.AddUint64(&h.numInvocations, 1)

	if result.FunctionError != nil {
		funcError := *result.FunctionError
		switch funcError {
		case "Unhandled":
			var msg unhandledErrorMessage
			if err := json.Unmarshal(result.Payload, &msg); err != nil {
				return nil, err
			}
			if msg.ErrorMessage == "" {
				return nil, fmt.Errorf("aws.Invoke: invalid unhandled error message: %s", result.Payload)
			}

			if strings.Contains(msg.ErrorMessage, "Task timed out") {
				// TODO(zhangwen): this is ugly; should maybe look into AWS Step Functions.
				return nil, &cp.HandlerTimeoutError{Handler: h}
			}

			// Otherwise, treat the error (e.g., memory limit exceeded) as "handler crashed".
			// TODO(zhangwen): is this reasonable?
			fallthrough
		case "Handled": // Handler raised exception.
			return nil, &cp.HandlerCrashedError{Handler: h, Message: string(result.Payload)}
		}

		return nil, fmt.Errorf("aws.Invoke: unrecognized FunctionError: %s", funcError)
	}

	return result.Payload, nil
}

// Finalize produces the log and deletes the lambda handler function created earlier and its log group.
func (h *handler) Finalize() {
	// Delete lambda handler.
	delFunc := &lambda.DeleteFunctionInput{
		FunctionName: aws.String(h.functionName),
	}
	if _, err := h.svc.DeleteFunction(delFunc); err != nil {
		log.Println("aws.Finalize: delete function error:", err)
	} else {
		log.Println("aws.Finalize: lambda function deleted:", h.functionName)
	}

	if h.shouldLog {
		// Gather logs from all invocations and write to logWriter.
		svcLogs := cloudwatchlogs.New(h.sess)
		logGroupName, err := h.gatherLog(svcLogs)
		if err != nil {
			log.Println("aws.Finalize: gather log error:", err)
		}

		// Delete log group.
		const deleteLogGroupWait = 5 * time.Second
		log.Printf("aws.Finalize: waiting %v before deleting log group...", deleteLogGroupWait)
		time.Sleep(deleteLogGroupWait)
		delLogGroup := &cloudwatchlogs.DeleteLogGroupInput{LogGroupName: aws.String(logGroupName)}
		if _, err := svcLogs.DeleteLogGroup(delLogGroup); err != nil {
			log.Printf("aws.Finalize: delete log group %q error: %v", logGroupName, err)
		} else {
			log.Println("aws.Finalize: log group delete finished:", logGroupName)
		}

		// If !h.shouldLog, the lambda wasn't given CloudWatch access in the first place.  Consequently, no log group could
		// have been created by this handler, and so no cleanup is necessary.
	}
}

// zipPackage returns the contents of a zip file containing the specified files.
// Also returns the name of the handler module, assuming the first file contains the handler function.
// E.g., if the first file is named "factorial.py", then the handler module is "factorial".
func zipPackage(filePaths []string) (zip []byte, handlerModule string, err error) {
	if len(filePaths) == 0 {
		err = fmt.Errorf("aws.zipPackage: deploy package cannot be empty")
		return nil, "", err
	}

	// Convert paths into absolute paths to get desired directory structure in zip.
	absPaths := make([]string, len(filePaths))
	for i, p := range filePaths {
		if i == 0 { // The first file is assumed to be the handler script.
			base := filepath.Base(p)
			ext := filepath.Ext(base)
			if ext != ".py" {
				err = fmt.Errorf("aws.zipPackage: the first file %s is not a Python script", p)
				return nil, "", err
			}
			handlerModule = strings.TrimSuffix(base, ext)
		}

		if absPaths[i], err = filepath.Abs(p); err != nil {
			return nil, "", err
		}
	}

	// Compress the files.
	var b bytes.Buffer
	if err = archiver.Zip.Write(&b, absPaths); err != nil {
		return nil, "", err
	}

	zip = b.Bytes()
	return zip, handlerModule, nil
}

// getIAMRole creates an IAM role for Kappa lambda handlers or, if one exists, returns the existing role.
// The role will have all necessary permissions to, e.g., storage.  It will not be deleted at cleanup time.
func getIAMRole(sess *session.Session, shouldLog bool) (*iam.Role, error) {
	var roleName string
	policyARNs := []string{s3ARN} // All lambdas should be given S3 access.
	if shouldLog {
		roleName = iamLoggingRoleName
		policyARNs = append(policyARNs, loggingARN) // In addition, grant CloudWatch access.
	} else {
		roleName = iamNoLoggingRoleName
	}

	svc := iam.New(sess)
	role, err := fetchOrCreateIAMRole(svc, roleName)
	if err != nil {
		return nil, err
	}

	for _, arn := range policyARNs {
		// Ensure that the IAM role has necessary permissions; code is no-op if permissions are already present.
		input := &iam.AttachRolePolicyInput{
			RoleName:  role.RoleName,
			PolicyArn: aws.String(arn),
		}
		if _, err := svc.AttachRolePolicy(input); err != nil {
			return nil, err
		}
	}

	return role, nil
}

// fetchOrCreateIAMRole is a helper function that fetches the Kappa IAM role, creating one if role doesn't exist.
// Caveat: lambda creation might fail if its IAM role is created too recently; this problem is dealt with by having lambda
// creation do retries on failure.
// FIXME(zhangwen): this delay can also cause runtime failures, which is not dealt with right now...
func fetchOrCreateIAMRole(svc *iam.IAM, roleName string) (*iam.Role, error) {
	// Try creating the role.
	input := &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(iamTrustPolicy),
		RoleName:                 aws.String(roleName),
		Description:              aws.String(iamRoleDescription),
	}
	result, err := svc.CreateRole(input)
	if err == nil {
		log.Println("fetchOrCreateIAMRole: IAM role created:", roleName)
		return result.Role, nil
	}

	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == iam.ErrCodeEntityAlreadyExistsException {
		// The IAM role already exists, so just get it.
		input := &iam.GetRoleInput{RoleName: aws.String(roleName)}
		result, err := svc.GetRole(input)
		if err != nil {
			return nil, fmt.Errorf(
				"fetchOrCreateIAMRole: error getting existing IAM role %q: %v", roleName, err)
		}

		log.Println("fetchOrCreateIAMRole: IAM role fetched:", roleName)
		return result.Role, nil
	}

	// Some unexpected error has occurred.
	return nil, fmt.Errorf("aws.fetchOrCreateIAMRole: %v", err)
}

// createLambdaFunction creates a lambda function under a generated unique name.
func createLambdaFunction(svc *lambda.Lambda, name string, codeZip []byte, env cp.EnvT, handlerModule string,
	timeoutSecs int, role *iam.Role) (funcConfig *lambda.FunctionConfiguration, err error) {

	// Retry policy for creating lambda function.
	const maxTrials = 3
	const sleepDuration = 10 * time.Second

	// Incorporate randomness to make unique name.
	// Not using lambda versioning, which could make deletion weird.  (Who would be deleting the $LATEST version?)
	funcName := fmt.Sprintf("%s-%s", funcNamePrefix, util.MakeRandomASCII128())
	// Turn environment map into AWS struct type.
	envVars := make(map[string]*string)
	for k, v := range env {
		envVars[k] = aws.String(v)
	}
	// Name of lambda's entry point function.
	lambdaHandler := fmt.Sprintf("%s.%s", handlerModule, handlerFunction)

	input := &lambda.CreateFunctionInput{
		FunctionName: aws.String(funcName),
		Code:         &lambda.FunctionCode{ZipFile: codeZip},
		Environment:  &lambda.Environment{Variables: envVars},
		Handler:      aws.String(lambdaHandler),
		Timeout:      aws.Int64(int64(timeoutSecs)),
		Role:         role.Arn,
		Description:  aws.String(fmt.Sprintf("Kappa workload %s", name)),
		Runtime:      aws.String(lambdaRuntime),
		MemorySize:   aws.Int64(memorySizeMB),
	}

	// Try creating a lambda.  Lambda creation may fail if the IAM role was created too recently; retries should fix it.
	for i := 0; i < maxTrials; i++ {
		funcConfig, err = svc.CreateFunction(input)
		if err == nil {
			break
		}

		log.Printf("createLambdaFunction: lambda creation failed (trial %d/%d): %v", i+1, maxTrials, err)
		if i+1 < maxTrials {
			log.Printf("createLambdaFunction: retrying in %v...", sleepDuration)
			time.Sleep(sleepDuration)
		} else {
			log.Printf("createLambdaFunction: giving up!")
		}
	}

	return funcConfig, err
}

// gatherLog fetches all logs from previous invocations and writes them to the logWriter.
// No matter whether an error occurs, the log group name is always returned.
func (h *handler) gatherLog(svc *cloudwatchlogs.CloudWatchLogs) (logGroupName string, err error) {
	const sleepDuration = 10 * time.Second
	const maxTrials = 6

	// Poll until log from CloudWatch is complete.
	logGroupName = logGroupPrefix + h.functionName
	complete := false
	var messages []string
	for i := 0; i < maxTrials; i++ {
		// Just wait up front; the logs are unlikely to be ready immediately after the workload ends.
		log.Printf("gatherLog: waiting for %v...", sleepDuration)
		time.Sleep(sleepDuration)

		messages, err = fetchMessagesFromGroup(svc, logGroupName)
		if err != nil {
			return logGroupName, err
		}

		var totalInvocations, seenInvocations uint64
		complete, totalInvocations, seenInvocations = h.isLogComplete(messages)
		if complete {
			break
		}

		log.Printf("gatherLog: (trial %d/%d) log not complete (%d entries, %v/%v invocations)", i+1, maxTrials,
			len(messages), seenInvocations, totalInvocations)
	}

	if complete {
		log.Printf("gatherLog: log gathered successfully (%d entries)", len(messages))
	} else {
		log.Printf("gatherLog: WARNING: log (%d entries) not complete after %d trials; proceeding anyway!",
			len(messages), maxTrials)
	}

	// Make one call to logWriter.Write to prevent taking a mutex multiple times.
	h.logWriter.Write([]byte(strings.Join(messages, "")))
	return logGroupName, nil
}

// isLogComplete returns true if the messages include all logs from previous invocations.
// TODO(zhangwen): when this function is called, some processes may not have completed.
func (h *handler) isLogComplete(messages []string) (complete bool, totalInvocations uint64, seenInvocations uint64) {
	// Count the number of invocations recorded in the logs.
	seenInvocations = 0
	seenRequestIDs := make(map[string]bool)
	tail := regexp.MustCompile( // Regex for (prefix of) last log of an invocation.
		`REPORT RequestId: ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s+Duration:`)
	for _, message := range messages {
		match := tail.FindStringSubmatch(message)
		if match == nil {
			continue
		}

		// Just double-checking that there're no duplicate request IDs.
		requestID := match[1]
		if _, ok := seenRequestIDs[requestID]; ok {
			log.Println("isLogComplete: WARNING: duplicate request ID:", requestID)
			continue
		}

		seenRequestIDs[requestID] = true
		seenInvocations += 1
	}

	totalInvocations = atomic.LoadUint64(&h.numInvocations)
	return seenInvocations == totalInvocations, totalInvocations, seenInvocations
}

// fetchMessagesFromGroup returns a list of all messages in all log streams in a log group.
func fetchMessagesFromGroup(svc *cloudwatchlogs.CloudWatchLogs, logGroup string) ([]string, error) {
	var logStreams []*cloudwatchlogs.LogStream
	input := &cloudwatchlogs.DescribeLogStreamsInput{LogGroupName: aws.String(logGroup)}
	err := svc.DescribeLogStreamsPages(input,
		func(page *cloudwatchlogs.DescribeLogStreamsOutput, _ bool) bool {
			logStreams = append(logStreams, page.LogStreams...)
			return true // Keep iterating.
		})
	if err != nil {
		return nil, fmt.Errorf("fetchMessagesFromGroup (%s): %v", logGroup, err)
	}

	var allMessages []string
	for _, stream := range logStreams {
		messages, err := fetchMessagesFromStream(svc, logGroup, *stream.LogStreamName)
		if err != nil {
			return nil, err
		}
		allMessages = append(allMessages, messages...)
	}

	return allMessages, nil
}

// fetchMessagesFromStream returns a list of all messages in a log stream.
func fetchMessagesFromStream(svc *cloudwatchlogs.CloudWatchLogs, logGroup string, logStream string) ([]string, error) {
	var messages []string
	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
		StartFromHead: aws.Bool(true),
	}

	var lastToken string
	err := svc.GetLogEventsPages(input,
		func(page *cloudwatchlogs.GetLogEventsOutput, _ bool) bool {
			for _, event := range page.Events {
				messages = append(messages, *event.Message)
			}

			if lastToken == *page.NextForwardToken { // No more log entries.
				return false
			}

			lastToken = *page.NextForwardToken
			return true
		})

	if err != nil {
		return nil, fmt.Errorf("fetchMessagesFromStream (%s, %s): %v", logGroup, logStream, err)
	}

	return messages, nil
}
