package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"golang.org/x/sys/unix"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	sess *session.Session
	svc  *rds.RDS
)

func init() {
	sess = session.Must(session.NewSession())
	svc = rds.New(sess)
}

func delay() time.Duration {
	return 25*time.Second + time.Duration(rand.Int63n(5000))*time.Millisecond
}

func dbStatus(ctx context.Context, instanceID string) (string, error) {
	req := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instanceID),
		MaxRecords:           aws.Int64(20),
	}
	res, err := svc.DescribeDBInstancesWithContext(ctx, req)
	if err != nil {
		return "", err
	}

	if len(res.DBInstances) == 0 {
		return "", fmt.Errorf("no such instance: %s", instanceID)
	}
	if len(res.DBInstances) > 1 {
		return "", errors.New("DescribeDBInstances query matched multiple instances")
	}
	status := res.DBInstances[0].DBInstanceStatus
	if status == nil {
		return "", fmt.Errorf("no status for instance: %s", instanceID)
	}
	return *status, nil
}

func waitUntilDBAvailable(ctx context.Context, instanceID string) error {
	for {
		status, err := dbStatus(ctx, instanceID)
		if err != nil {
			return err
		}
		log.Printf("instance status: %s", status)
		if status == "available" {
			break
		}

		select {
		case <-time.After(delay()):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func main() {
	var (
		app          = kingpin.New("wait-until-aws-rds-available", "Block until an AWS RDS instance transitions into available state.")
		instanceID   = app.Arg("db-instance-identifier", "AWS RDS DBInstanceIdentifier of the instance to watch.").Required().String()
		ignoreErrors = app.Flag("ignore-aws-errors", "Retry on errors from the AWS SDK.").Bool()
	)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, unix.SIGINT, unix.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
		case s := <-signals:
			log.Printf("%s received - terminating...", s)
			cancel()
		}
	}()

	var err error
retry:
	for {
		err = waitUntilDBAvailable(ctx, *instanceID)
		if err == nil {
			break retry
		}
		switch awsErr := err.(type) {
		case awserr.Error:
			if awsErr.Code() == "RequestCanceled" {
				break retry
			}
		default:
			break retry
		}
		if !*ignoreErrors {
			break retry
		}
		log.Printf("retrying: %v", err)

		select {
		case <-time.After(delay()):
		case <-ctx.Done():
			err = ctx.Err()
			break retry
		}
	}
	if err != nil {
		log.Fatal(err)
	}
}
