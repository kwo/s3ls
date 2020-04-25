package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kelseyhightower/envconfig"
)

const (
	AppName = "s3ls"
)

type Config struct {
	AccessKey       string `required:"true"`
	SecretAccessKey string `required:"true"`
	Region          string `required:"true"`
	Bucket          string `required:"true"`
	Workers         int
}

type Item struct {
	Key         string
	ContentType string
}

func main() {

	cfg := &Config{}
	envconfig.MustProcess(AppName, cfg)
	if cfg.Workers < 1 {
		cfg.Workers = runtime.NumCPU()
	}

	awsSession := session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretAccessKey, ""),
		Region:      &cfg.Region,
	}))
	s3Session := s3.New(awsSession, &aws.Config{})

	ctx, killSwitch := exitContext()

	listBucketContents(ctx, killSwitch, s3Session, cfg.Bucket, cfg.Workers)

}

func listBucketContents(ctx context.Context, killSwitch func(error), s3Session *s3.S3, bucketName string, workerCount int) {

	objects := listObjects(ctx, killSwitch, s3Session, bucketName)

	workers := make([]<-chan Item, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = fetchMetadata(ctx, killSwitch, s3Session, bucketName, objects)
	}
	items := merge(workers...)

	for item := range items {
		fmt.Printf("%s %s\n", item.Key, item.ContentType)
	}

}

func listObjects(ctx context.Context, killSwitch func(error), s3Session *s3.S3, bucketName string) <-chan s3.Object {

	out := make(chan s3.Object)

	go func() {
		listing, err := s3Session.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{Bucket: &bucketName})
		if err != nil {
			killSwitch(fmt.Errorf("cannot list objects: %w", err))
		} else {
			for _, object := range listing.Contents {
				if object != nil {
					out <- *object
				}
			}
		}
		close(out)
	}()

	return out

}

func fetchMetadata(ctx context.Context, killSwitch func(error), s3Session *s3.S3, bucketName string, in <-chan s3.Object) <-chan Item {

	out := make(chan Item)

	go func() {
		for object := range in {
			head, err := s3Session.HeadObjectWithContext(ctx, &s3.HeadObjectInput{Bucket: &bucketName, Key: object.Key})
			if err != nil {
				killSwitch(err)
				return
			}
			out <- Item{
				Key:         *object.Key,
				ContentType: *head.ContentType,
			}
		}
		close(out)
	}()

	return out

}

func merge(workers ...<-chan Item) <-chan Item {

	out := make(chan Item)

	var wg sync.WaitGroup
	wg.Add(len(workers))

	copier := func(items <-chan Item) {
		for item := range items {
			out <- item
		}
		wg.Done()
	}

	for _, worker := range workers {
		go copier(worker)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out

}

func exitContext() (context.Context, func(error)) {

	ctx, cancel := context.WithCancel(context.Background())
	killSwitch := func(err error) {
		cancel()
		if err != nil {
			fmt.Println(err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-c:
			cancel()
			fmt.Println()
			return
		}
	}()

	return ctx, killSwitch

}
