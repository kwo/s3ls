package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Item struct {
	Object   s3.Object
	Metadata *s3.HeadObjectOutput
}

type Entry struct {
	Key                       string            `json:"key"`
	Invalid                   bool              `json:"invalid,omitempty"`
	Size                      int64             `json:"size"`
	OwnerID                   string            `json:"ownerId,omitempty"`
	OwnerName                 string            `json:"ownerName,omitempty"`
	ETagObject                string            `json:"etagObject,omitempty"`
	LastModifiedObject        *time.Time        `json:"lastModifiedObject,omitempty"`
	StorageClassObject        string            `json:"storageClassObject,omitempty"`
	AcceptRanges              string            `json:"acceptRanges,omitempty"`
	CacheControl              string            `json:"cacheControl,omitempty"`
	ContentDisposition        string            `json:"contentDisposition,omitempty"`
	ContentEncoding           string            `json:"contentEncoding,omitempty"`
	ContentLanguage           string            `json:"contentLanguage,omitempty"`
	ContentLength             int64             `json:"contentLength,omitempty"`
	ContentType               string            `json:"contentType,omitempty"`
	DeleteMarker              bool              `json:"deleteMarker,omitempty"`
	ETag                      string            `json:"etag,omitempty"`
	Expiration                string            `json:"expiration,omitempty"`
	Expires                   string            `json:"expires,omitempty"`
	LastModified              *time.Time        `json:"lastModified,omitempty"`
	Metadata                  map[string]string `json:"metadata,omitempty"`
	MissingMeta               int64             `json:"missingMeta,omitempty"`
	ObjectLockLegalHoldStatus string            `json:"objectLockLegalHoldStatus,omitempty"`
	ObjectLockMode            string            `json:"objectLockMode,omitempty"`
	ObjectLockRetainUntilDate *time.Time        `json:"objectLockRetainUntilDate,omitempty"`
	PartsCount                int64             `json:"partsCount,omitempty"`
	ReplicationStatus         string            `json:"replicationStatus,omitempty"`
	RequestCharged            string            `json:"requestCharged,omitempty"`
	Restore                   string            `json:"restore,omitempty"`
	SSECustomerAlgorithm      string            `json:"sseCustomerAlgorithm,omitempty"`
	SSECustomerKeyMD5         string            `json:"sseCustomerKeyMD5,omitempty"`
	SSEKMSKeyId               string            `json:"sseKMSKeyId,omitempty"`
	ServerSideEncryption      string            `json:"serverSideEncryption,omitempty"`
	StorageClass              string            `json:"storageClass,omitempty"`
	VersionId                 string            `json:"versionId,omitempty"`
	WebsiteRedirectLocation   string            `json:"websiteRedirectLocation,omitempty"`
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("usage: s3ls <bucket-name>")
		os.Exit(1)
	}

	bucketName := os.Args[1]
	workers := runtime.NumCPU()

	awsSession := session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvProvider{}, &credentials.SharedCredentialsProvider{},
		}),
	}))
	s3Session := s3.New(awsSession, &aws.Config{})

	ctx, killSwitch := exitContext()
	listBucketContents(ctx, killSwitch, s3Session, bucketName, workers)

}

func listBucketContents(ctx context.Context, killSwitch func(error), s3Session *s3.S3, bucketName string, workerCount int) {

	objects := listObjects(ctx, killSwitch, s3Session, bucketName)

	workers := make([]<-chan Item, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = fetchMetadata(ctx, killSwitch, s3Session, bucketName, objects)
	}
	items := merge(workers...)

	entries := toEntries(items)
	printEntriesAsJson(os.Stdout, entries)

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
				Object:   object,
				Metadata: head,
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

func toEntries(items <-chan Item) <-chan Entry {

	out := make(chan Entry)

	go func() {
		for item := range items {
			entry := Entry{
				Key:                aws.StringValue(item.Object.Key),
				Size:               aws.Int64Value(item.Object.Size),
				ETagObject:         aws.StringValue(item.Object.ETag),
				LastModifiedObject: item.Object.LastModified,
				StorageClassObject: aws.StringValue(item.Object.StorageClass),
			}
			if item.Object.Owner != nil {
				entry.OwnerID = aws.StringValue(item.Object.Owner.ID)
				entry.OwnerName = aws.StringValue(item.Object.Owner.DisplayName)
			}
			entry.Invalid = item.Metadata == nil
			if !entry.Invalid {
				entry.AcceptRanges = aws.StringValue(item.Metadata.AcceptRanges)
				entry.CacheControl = aws.StringValue(item.Metadata.CacheControl)
				entry.ContentDisposition = aws.StringValue(item.Metadata.ContentDisposition)
				entry.ContentEncoding = aws.StringValue(item.Metadata.ContentEncoding)
				entry.ContentLanguage = aws.StringValue(item.Metadata.ContentLanguage)
				entry.ContentLength = aws.Int64Value(item.Metadata.ContentLength)
				entry.ContentType = aws.StringValue(item.Metadata.ContentType)
				entry.DeleteMarker = aws.BoolValue(item.Metadata.DeleteMarker)
				entry.ETag = aws.StringValue(item.Metadata.ETag)
				entry.Expiration = aws.StringValue(item.Metadata.Expiration)
				entry.Expires = aws.StringValue(item.Metadata.Expires)
				entry.LastModified = item.Metadata.LastModified
				entry.Metadata = aws.StringValueMap(item.Metadata.Metadata)
				entry.MissingMeta = aws.Int64Value(item.Metadata.MissingMeta)
				entry.ObjectLockLegalHoldStatus = aws.StringValue(item.Metadata.ObjectLockLegalHoldStatus)
				entry.ObjectLockMode = aws.StringValue(item.Metadata.ObjectLockMode)
				entry.ObjectLockRetainUntilDate = item.Metadata.ObjectLockRetainUntilDate
				entry.PartsCount = aws.Int64Value(item.Metadata.PartsCount)
				entry.ReplicationStatus = aws.StringValue(item.Metadata.ReplicationStatus)
				entry.RequestCharged = aws.StringValue(item.Metadata.RequestCharged)
				entry.Restore = aws.StringValue(item.Metadata.Restore)
				entry.SSECustomerAlgorithm = aws.StringValue(item.Metadata.SSECustomerAlgorithm)
				entry.SSECustomerKeyMD5 = aws.StringValue(item.Metadata.SSECustomerKeyMD5)
				entry.SSEKMSKeyId = aws.StringValue(item.Metadata.SSEKMSKeyId)
				entry.ServerSideEncryption = aws.StringValue(item.Metadata.ServerSideEncryption)
				entry.StorageClass = aws.StringValue(item.Metadata.StorageClass)
				entry.VersionId = aws.StringValue(item.Metadata.VersionId)
				entry.WebsiteRedirectLocation = aws.StringValue(item.Metadata.WebsiteRedirectLocation)
			}
			out <- entry
		}
		close(out)
	}()

	return out

}

func printEntriesAsJson(w io.Writer, entries <-chan Entry) {
	notRowOne := false
	_, _ = w.Write([]byte("["))
	for entry := range entries {
		if notRowOne {
			_, _ = w.Write([]byte(","))
		}
		data, _ := json.Marshal(entry)
		_, _ = w.Write(data)
		notRowOne = true
	}
	_, _ = w.Write([]byte("]"))
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
