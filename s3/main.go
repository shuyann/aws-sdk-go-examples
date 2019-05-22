package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"strings"
)

func main() {
	// Create session using the default region and credentials
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := s3.New(sess)
	listBuckets(svc)
}

func createNewBucketAndObject(svc *s3.S3, bucket, key string) {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		exitErrorf("Failed to create bucket %v", err)
	}

	if err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &bucket}); err != nil {
		exitErrorf("Failed to wait for bucket to exist %s, %s\n", bucket, err)
	}

	_, err = svc.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader("Hello World!"),
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		exitErrorf("Failed to upload data to %s/%s, %s\n", bucket, key, err)
	}

	fmt.Printf("Successfully created bucket %s and uploaded data with key %s\n", bucket, key)
}

func listBuckets(svc *s3.S3) {
	// nil meas no filters are applied to the returned list
	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n", aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func listObjects(svc *s3.S3, bucket string) {
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	for _, item := range resp.Contents {
		fmt.Println("Name: ", *item.Key)
		fmt.Println("Last Modified: ", *item.LastModified)
		fmt.Println("Size: ", *item.Size)
		fmt.Println("Storage class: ", *item.StorageClass)
		fmt.Println("")
	}
}

func createBucket(svc *s3.S3, bucket string) {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		exitErrorf("Unable to create bucket %q, %v", bucket, err)
	}

	// Wait until bucket is created before finishing
	fmt.Printf("Waiting for bucket %q to be created...\n", bucket)

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		exitErrorf("Error occurred while waiting for bucket to be created %v", bucket)
	}

	fmt.Printf("Bucket %q successfully created\n", bucket)
}

func deleteBucket(svc *s3.S3, bucket string) {
	// Delete the s3 bucket
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		exitErrorf("Unable to delete bucket %q, %v", bucket, err)
	}

	// wait until bucket is deleted before finishing
	fmt.Printf("Waiting for bucket %q to be deleted...\n", bucket)
	err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		exitErrorf("Error occurred while waiting for bucket to be deleted, %v", bucket)
	}

	fmt.Printf("Bucket %q successfully deleted\n", bucket)
}

func deleteObject(svc *s3.S3, bucket, key string) {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		exitErrorf("Unable to delete object %q from bucket %q, %v", key, bucket, err)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		exitErrorf("Error occurred while waiting for object %q to be deleted, %v", key, err)
	}

	fmt.Printf("Object %q successfully deleted\n", key)
}

func copyObject(svc *s3.S3, from, bucket, key string) {
	// copy the item
	_, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucket), CopySource: aws.String(from), Key: aws.String(key)})
	if err != nil {
		exitErrorf("Unable to copy item from bucket %q to bucket %q, %v", bucket, from, err)
	}

	// wait to see if the item got copied
	err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		exitErrorf("Error occurred while waiting for item %q to be copied to bucket %q, %v", bucket, key, from, err)
	}

	fmt.Printf("Item %q successfully copied from bucket %q to bucket %q\n", key, bucket, from)
}

func uploadObject(uploader *s3manager.Uploader, bucket string) {
	fileName := "test.txt"
	file, err := os.Open(fileName)
	if err != nil {
		exitErrorf("Unable to open upload file %q, %v", fileName, err)
	}
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
		Body:   file,
	})
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", fileName, bucket, err)
	}
	fmt.Printf("Successfully uploaded %q to %q\n", fileName, bucket)
}

func downloadObject(downloader *s3manager.Downloader, bucket string) {
	fileName := "download.txt"
	file, err := os.Create(fileName)
	if err != nil {
		exitErrorf("Unable to create download file %q, %v", fileName, err)
	}
	item := "test.txt"
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", item, err)
	}
	fmt.Println("Donwloaded", file.Name(), numBytes, "bytes")
}

// Create function display errors and exit
func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
