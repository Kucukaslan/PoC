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

var (
	bucket = aws.String("aev-autonomous-driving-dataset")
	key    = aws.String("camera_lidar-20180810150607_bus_signals.tar") //tutorial.html") //camera_lidar_semantic_bus.tar") //"camera_lidar-20190401121727_lidar_frontcenter.tar")
	region = aws.String("eu-central-1")
)

func main() {
	flag.StringVar(bucket, "bucket", *bucket, "bucket name")
	flag.StringVar(key, "key", *key, "object key")
	flag.StringVar(region, "region", *region, "bucket region")
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.Credentials = aws.AnonymousCredentials{}
		options.Region = *region
	})

	f, err := os.Create(*key)
	if err != nil {
		log.Fatalf("unable to create file, %v", err)
	}
	fmt.Println(f.Name())

	obj, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		log.Fatalf("unable to get object attr, %v", err)
	}

	fmt.Println(*obj.ETag, obj.ContentLength)

	var r int64 = 0
	var partSize int64 = 32 * 1024 * 1024 // 32 MB
	size := obj.ContentLength

	var wg sync.WaitGroup
	for r < size {
		fmt.Printf("r: %v, size: %v\n", r, size)
		wg.Add(1)
		rStart := r
		r = Min(r+partSize, size)
		go func(start, end int64) {
			fmt.Printf("start: %v, end: %v\n", start, end)
			defer wg.Done()
			rng := strconv.FormatInt(start, 10) + "-" + strconv.FormatInt(end-1, 10)
			fmt.Println("Range", rng)
			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: bucket,
				Key:    key,
				Range:  aws.String("bytes=" + rng),
			})
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			buf := bytes.NewBuffer(make([]byte, 0, end-start))
			n, err := io.Copy(buf, out.Body)
			fmt.Printf("n: %v, err: %v, length: %v, cont lem: %v\n", n, err, buf.Len(), out.ContentLength)
			f.WriteAt(buf.Bytes(), start)
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
