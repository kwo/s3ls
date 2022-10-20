package s3ls

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog"
)

func Push(ctx context.Context, logger zerolog.Logger, s3Client *s3.Client, workerCount int, entries <-chan Entry) <-chan Entry {
	workers := make([]<-chan Entry, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = chattr(ctx, logger, s3Client, entries)
	}
	return mergeEntries(workers...)
}

func chattr(ctx context.Context, logger zerolog.Logger, s3Client *s3.Client, in <-chan Entry) <-chan Entry {
	out := make(chan Entry)
	go func() {
		defer close(out)
		for entry := range in {
			_, err := s3Client.CopyObject(ctx, &s3.CopyObjectInput{
				CopySource:        aws.String(urlEncode(fmt.Sprintf("%s/%s", entry.Bucket, entry.Key))),
				Bucket:            aws.String(entry.Bucket),
				Key:               aws.String(entry.Key),
				MetadataDirective: types.MetadataDirectiveReplace,

				CacheControl:            toNilString(entry.CacheControl),
				ContentDisposition:      toNilString(entry.ContentDisposition),
				ContentEncoding:         toNilString(entry.ContentEncoding),
				ContentLanguage:         toNilString(entry.ContentLanguage),
				ContentType:             toNilString(entry.ContentType),
				Expires:                 entry.Expires,
				Metadata:                entry.Metadata,
				WebsiteRedirectLocation: toNilString(entry.WebsiteRedirectLocation),
			})
			if err != nil {
				logger.Fatal().Err(err).Str("fn", "chattr").Msg("cannot push metadata")
				return
			}
			out <- entry
		}
	}()
	return out
}

func mergeEntries(workers ...<-chan Entry) <-chan Entry {
	out := make(chan Entry)

	var wg sync.WaitGroup
	wg.Add(len(workers))
	go func() {
		wg.Wait()
		close(out)
	}()

	copier := func(items <-chan Entry) {
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

func toNilString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func urlEncode(value string) string {
	u := url.URL{Path: value}
	return u.String()
}
