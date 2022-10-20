package s3ls

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type (
	mutator func(Entry) Entry
	matcher func(Entry) bool
)

const oneMonth = time.Hour * 24 * 30

// see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control

func Mutate(ctx context.Context, entries <-chan Entry) <-chan Entry {
	return mutate(ctx,
		entries,
		HTMLContentType,
		cacheControl(matchPrefix("fonts/"), immutable(oneMonth)),
		cacheControl(matchPrefix("img/"), immutable(oneMonth)),
		cacheControl(matchSuffix(".js"), immutable(oneMonth)),
		cacheControl(matchSuffix(".css"), immutable(oneMonth)),
		cacheControl(matchSuffix(".html"), "no-cache"),
	)
}

func mutate(ctx context.Context, entries <-chan Entry, mutations ...mutator) <-chan Entry {
	out := make(chan Entry)

	go func() {
		defer close(out)
		for entry := range entries {
			select {
			case <-ctx.Done():
				return
			default:
				for _, mutation := range mutations {
					entry = mutation(entry)
				}
				if entry.Touched {
					out <- entry
				}
			}
		}
	}()

	return out
}

// HTMLContentType ensures .html files and files without an extension have the proper content-type/-encoding.
func HTMLContentType(e Entry) Entry {
	if (!strings.Contains(e.Key, ".") || strings.HasSuffix(e.Key, ".html")) &&
		(e.ContentType != "text/html" || e.ContentEncoding != "utf-8") {
		e.ContentType = "text/html"
		e.ContentEncoding = "utf-8"
		e.Touched = true
	}
	return e
}

// nolint:unused
func matchFilename(filename string) matcher {
	return func(e Entry) bool {
		return e.Key == filename
	}
}

func matchPrefix(prefix string) matcher {
	return func(e Entry) bool {
		return strings.HasPrefix(e.Key, prefix)
	}
}

func matchSuffix(suffix string) matcher {
	return func(e Entry) bool {
		return strings.HasSuffix(e.Key, suffix)
	}
}

// nolint:unused
func matchUntouched() matcher {
	return func(e Entry) bool {
		return !e.Touched
	}
}

func immutable(d time.Duration) string {
	return fmt.Sprintf("public, max-age=%d, immutable", int(d.Seconds()))
}

func cacheControl(m matcher, cc string) mutator {
	return func(e Entry) Entry {
		if m(e) {
			if e.CacheControl != cc {
				e.CacheControl = cc
				e.Touched = true
			}
		}
		return e
	}
}

// nolint:unused
func contentType(m matcher, contentType string) mutator {
	return func(e Entry) Entry {
		if m(e) {
			if e.ContentType != contentType {
				e.ContentType = contentType
				e.Touched = true
			}
		}
		return e
	}
}
