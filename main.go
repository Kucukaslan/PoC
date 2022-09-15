package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const name = "PoCSequentiality"

var (
	bucket      = aws.String("aev-autonomous-driving-dataset")
	key         = aws.String("tutorial.html") //camera_lidar-20180810150607_bus_signals.tar") //camera_lidar_semantic_bus.tar") //"camera_lidar-20190401121727_lidar_frontcenter.tar")
	region      = aws.String("eu-central-1")
	concurrency = 16
	partSize    = int64(32 * 1024 * 1024) // 32 MiB
	logEnabled  bool
	notAnon     bool
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s is a simple CLI tool that aims to increase AWS S3 download throughput by taking advantage of sequential writes in addition to concurreny.\n", name)
		fmt.Fprintf(flag.CommandLine.Output(), "%s is designed to be Proof of Concept and prototype not an end product in any way.\n", name)
		fmt.Fprintln(flag.CommandLine.Output())
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", name)
		flag.PrintDefaults()
	}

	flag.BoolVar(&notAnon, "notanon", false, "use default credentials instead of anonymous credentials")
	flag.BoolVar(&logEnabled, "log", false, "whether logs will be printed")
	flag.StringVar(bucket, "bucket", aws.ToString(bucket), "bucket name")
	flag.StringVar(key, "key", aws.ToString(key), "object key")
	flag.StringVar(region, "region", aws.ToString(region), "bucket region")
	flag.IntVar(&concurrency, "concurrency", concurrency, "concurrency")
	flag.Int64Var(&partSize, "partsize", partSize, "bucket region")

	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if logEnabled {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	} else {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	fnsl := make([]func(options *s3.Options), 0, 2)
	fnsl = append(fnsl, func(options *s3.Options) {
		options.Region = aws.ToString(region)
	})

	if !notAnon {
		fnsl = append(fnsl, func(options *s3.Options) {
			options.Credentials = aws.AnonymousCredentials{}
		})
	}
	client := s3.NewFromConfig(cfg, fnsl...)

	f, err := os.Create(*key)
	if err != nil {
		log.Fatalf("unable to create file, %v", err)
	}
	log.Printf("file: %q\n", f.Name())

	obj, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		log.Fatalf("unable to get object attr, %v", err)
	}

	log.Printf("Etag %s, size in bytes %d, size in KB ~%d, size in MB ~%d\n", *obj.ETag, obj.ContentLength, obj.ContentLength>>10, obj.ContentLength>>20)

	var r int64 = 0
	size := obj.ContentLength

	sem := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		sem <- struct{}{}
	}
	var wg sync.WaitGroup
	// there is no enforced limitation in number of workers but the number of
	// concurrently working workers are limited with semaphore "sem"
	for r < size {
		log.Printf("r: %v, size: %v\n", r, size)
		wg.Add(1)
		rStart := r
		r = Min(r+partSize, size)
		go func(start, end int64) {
			<-sem
			defer func() {
				sem <- struct{}{}
				wg.Done()
			}()
			rng := strconv.FormatInt(start, 10) + "-" + strconv.FormatInt(end-1, 10)
			log.Printf("Range: %20s, %20s\n", rng, "BEGIN")
			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: bucket,
				Key:    key,
				Range:  aws.String("bytes=" + rng),
			})
			log.Printf("Range: %20s, %20s\n", rng, "DOWNLOADED")

			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			log.Printf("Range: %20s, %20s\n", rng, "COPY BUFFER")
			buf := bytes.NewBuffer(make([]byte, 0, end-start))
			io.Copy(buf, out.Body)
			log.Printf("Range: %20s, %20s\n", rng, "STARTED WRITING")
			f.WriteAt(buf.Bytes(), start)
			log.Printf("Range: %20s, %20s\n", rng, "WROTE")
		}(rStart, r)
	}
	wg.Wait()
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
