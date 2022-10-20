package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"

	"github.com/kwo/s3ls/pkg/s3ls"
)

func main() {
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	fatalHook := func(e *zerolog.Event, level zerolog.Level, message string) {
		if level == zerolog.FatalLevel {
			cancelFn()
		}
	}
	consoleWriter := zerolog.ConsoleWriter{
		TimeFormat: "15:04:05",
		Out:        os.Stdout,
	}
	log := zerolog.New(consoleWriter).With().Timestamp().Logger().Hook(zerolog.HookFunc(fatalHook))

	const zeroArguments = 1
	if len(os.Args) <= zeroArguments {
		fmt.Println("usage: s3ls <bucket-name>")
		os.Exit(1)
	}

	bucketName := os.Args[1]
	doMutations := len(os.Args) > 2 && os.Args[2] == "-x" // TODO: use real flag
	workers := runtime.NumCPU()

	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to load SDK config")
	}
	s3Session := s3.NewFromConfig(cfg)

	entries := s3ls.List(ctx, log, s3Session, bucketName, workers)
	if doMutations {
		entries = s3ls.Mutate(ctx, entries)
		entries = s3ls.Push(ctx, log, s3Session, workers, entries)
	}
	s3ls.EntriesToJSON(ctx, log, entries, os.Stdout)
}
