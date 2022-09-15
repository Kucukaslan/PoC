package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	bucket = aws.String("aev-autonomous-driving-dataset")
	key    = aws.String("tutorial.html") //"camera_lidar_semantic_bus.tar") //"camera_lidar-20190401121727_lidar_frontcenter.tar")
	region = aws.String("eu-central-1")
)

func main() {
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String("my-bucket"),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("first page results:")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
}

func mainO() {
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
	fmt.Println("config", cfg)

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.Credentials = aws.AnonymousCredentials{}
		options.Region = *region
	})
	fmt.Println("client", client)

	obj, err := client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket: bucket,
		Key:    key,
		ObjectAttributes: []types.ObjectAttributes{
			"ETag",
			"ObjectParts",
			"StorageClass",
			"ObjectSize",
		},
	})
	if err != nil {
		log.Fatalf("unable to get object attr, %v", err)
	}

	fmt.Println(obj)

	var r int64 = 0
	var partSize int64 = 32 * 1024 * 1024 // 32 MB
	size := obj.ObjectSize

	f, err := os.Create(*key)
	if err != nil {
		log.Fatalf("unable to create file, %v", err)
	}
	fmt.Println(f)

	var out *s3.GetObjectOutput
	for r < size {
		rStart := r
		r = Min(r+partSize, size)
		go func(start, end int64) {
			out, err = client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: bucket,
				Key:    key,
				Range:  aws.String(strconv.FormatInt(rStart, 10) + "-" + strconv.FormatInt((r), 10)),
			})
			if err != nil {
				log.Fatalf("unable to load SDK config, %v", err)
			}
			buf := make([]byte, partSize)
			out.Body.Read(buf)
			f.WriteAt(buf, rStart)
		}(rStart, r)
	}

	fmt.Println(out)
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
