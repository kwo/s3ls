# S3 Lister

List the contents of an S3 bucket.

A simple example app to demonstrate the fan-in/fan-out concurrency pattern in listing objects in an S3 bucket.
Metadata cannot be returned by the listing and must be requested separately for each object, 
so it is helpful to have a pool of workers for this task.

## Usage

```shell script
export DISPATCH_ACCESSKEY=<access-key>
export DISPATCH_SECRETACCESSKEY=<secret-access-key>
export DISPATCH_REGION=<region>
export DISPATCH_BUCKET=<bucket-name>
export DISPATCH_WORKERS=3

go run .
```
