package s3ls

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog"
)

func List(
	ctx context.Context, logger zerolog.Logger, s3Client *s3.Client, bucketName string, workerCount int,
) <-chan Entry {
	checkBucket(ctx, logger, s3Client, bucketName)
	objects := listObjects(ctx, logger, s3Client, bucketName)

	workers := make([]<-chan Item, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = fetchMetadata(ctx, logger, s3Client, bucketName, objects)
	}
	items := mergeItems(workers...)

	return toEntries(ctx, items)
}

func checkBucket(ctx context.Context, logger zerolog.Logger, s3Client *s3.Client, bucketName string) {
	_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		logger.Fatal().Err(err).Str("bucket", bucketName).Msg("cannot access bucket")
	}
}

func listObjects(ctx context.Context, logger zerolog.Logger, s3Client *s3.Client, bucketName string) <-chan types.Object {
	out := make(chan types.Object)

	go func() {
		defer close(out)
		listing, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucketName})
		if err != nil {
			logger.Fatal().Err(err).Str("fn", "listObjects").Msg("cannot list objects")
			return
		}
		for _, object := range listing.Contents {
			select {
			case <-ctx.Done():
				return
			default:
				out <- object
			}
		}
	}()

	return out
}

func fetchMetadata(
	ctx context.Context, logger zerolog.Logger, s3Client *s3.Client, bucketName string, in <-chan types.Object,
) <-chan Item {
	out := make(chan Item)

	go func() {
		defer close(out)
		for object := range in {
			select {
			case <-ctx.Done():
				return
			default:
				head, err := s3Client.HeadObject(
					ctx, &s3.HeadObjectInput{Bucket: &bucketName, Key: object.Key})
				if err != nil {
					logger.Fatal().Err(err).Str("fn", "fetchMetadata").Msg("cannot get head object")
					return
				}
				out <- Item{
					Bucket:   bucketName,
					Object:   object,
					Metadata: head,
				}
			} // select
		} // loop
	}()

	return out
}

func mergeItems(workers ...<-chan Item) <-chan Item {
	out := make(chan Item)

	var wg sync.WaitGroup
	wg.Add(len(workers))
	go func() {
		wg.Wait()
		close(out)
	}()

	copier := func(items <-chan Item) {
		for item := range items {
			out <- item
		}
		wg.Done()
	}

	for _, worker := range workers {
		go copier(worker)
	}

	return out
}

func toEntries(ctx context.Context, items <-chan Item) <-chan Entry { // nolint:maintidx
	out := make(chan Entry)

	go func() {
		defer close(out)
		for item := range items {
			entry := Entry{
				Key:                aws.ToString(item.Object.Key),
				Bucket:             item.Bucket,
				Size:               item.Object.Size,
				ETagObject:         aws.ToString(item.Object.ETag),
				LastModifiedObject: item.Object.LastModified,
				StorageClassObject: string(item.Object.StorageClass),
			}
			if item.Object.Owner != nil {
				entry.OwnerID = aws.ToString(item.Object.Owner.ID)
				entry.OwnerName = aws.ToString(item.Object.Owner.DisplayName)
			}
			entry.Invalid = item.Metadata == nil
			if !entry.Invalid {
				entry.AcceptRanges = aws.ToString(item.Metadata.AcceptRanges)
				entry.CacheControl = aws.ToString(item.Metadata.CacheControl)
				entry.ContentDisposition = aws.ToString(item.Metadata.ContentDisposition)
				entry.ContentEncoding = aws.ToString(item.Metadata.ContentEncoding)
				entry.ContentLanguage = aws.ToString(item.Metadata.ContentLanguage)
				entry.ContentLength = item.Metadata.ContentLength
				entry.ContentType = aws.ToString(item.Metadata.ContentType)
				entry.DeleteMarker = item.Metadata.DeleteMarker
				entry.ETag = aws.ToString(item.Metadata.ETag)
				entry.Expiration = aws.ToString(item.Metadata.Expiration)
				entry.Expires = item.Metadata.Expires
				entry.LastModified = item.Metadata.LastModified
				entry.Metadata = item.Metadata.Metadata
				entry.MissingMeta = item.Metadata.MissingMeta
				entry.ObjectLockLegalHoldStatus = string(item.Metadata.ObjectLockLegalHoldStatus)
				entry.ObjectLockMode = string(item.Metadata.ObjectLockMode)
				entry.ObjectLockRetainUntilDate = item.Metadata.ObjectLockRetainUntilDate
				entry.PartsCount = item.Metadata.PartsCount
				entry.ReplicationStatus = string(item.Metadata.ReplicationStatus)
				entry.RequestCharged = string(item.Metadata.RequestCharged)
				entry.Restore = aws.ToString(item.Metadata.Restore)
				entry.SSECustomerAlgorithm = aws.ToString(item.Metadata.SSECustomerAlgorithm)
				entry.SSECustomerKeyMD5 = aws.ToString(item.Metadata.SSECustomerKeyMD5)
				entry.SSEKMSKeyID = aws.ToString(item.Metadata.SSEKMSKeyId)
				entry.ServerSideEncryption = string(item.Metadata.ServerSideEncryption)
				entry.StorageClass = string(item.Metadata.StorageClass)
				entry.VersionID = aws.ToString(item.Metadata.VersionId)
				entry.WebsiteRedirectLocation = aws.ToString(item.Metadata.WebsiteRedirectLocation)
			}

			select {
			case out <- entry:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func EntriesToJSON(ctx context.Context, logger zerolog.Logger, entries <-chan Entry, w io.Writer) {
	addLeadingComma := false
	if _, err := w.Write([]byte("[")); err != nil {
		logger.Fatal().Err(err).Str("fn", "entriesToJSON").Msg("cannot write opening json array bracket")
		return
	}
	for entry := range entries {
		select {
		case <-ctx.Done():
			return
		default:
			if addLeadingComma {
				if _, err := w.Write([]byte(",\n")); err != nil {
					logger.Fatal().Err(err).Str("fn", "entriesToJSON").Msg("cannot add leading comma")
					return
				}
			}
			data, err := json.Marshal(entry)
			if err != nil {
				logger.Fatal().Err(err).Str("fn", "entriesToJSON").Msg("cannot serialize entry")
				return
			}
			if _, err := w.Write(data); err != nil {
				logger.Fatal().Err(err).Str("fn", "entriesToJSON").Msg("cannot write data")
				return
			}
			addLeadingComma = true
		}
	}
	if _, err := w.Write([]byte("]")); err != nil {
		logger.Fatal().Err(err).Str("fn", "entriesToJSON").Msg("cannot write closing json array bracket")
		return
	}
}
