# S3 Lister

List the contents of an S3 bucket.

A simple example app to demonstrate the fan-in/fan-out concurrency pattern in listing objects in an S3 bucket.
Metadata cannot be returned by the listing and must be requested separately for each object, 
so it is helpful to have a pool of workers for this task.

## Usage

```shell script
export S3LS_ACCESSKEY=<access-key>
export S3LS_SECRETACCESSKEY=<secret-access-key>
export S3LS_REGION=<region>
export S3LS_BUCKET=<bucket-name>
export S3LS_WORKERS=3

go run .
```
