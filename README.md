# S3 Lister

List the contents of an S3 bucket.

A simple example app to demonstrate the fan-in/fan-out concurrency pattern in listing objects in an S3 bucket.
Metadata cannot be returned by the listing and must be requested separately for each object, 
so it is helpful to have a pool of workers for this task.

## Usage

Set the following environment variables to set the AWS credentials:

`AWS_ACCESS_KEY_ID` or `AWS_ACCESS_KEY`

`AWS_SECRET_ACCESS_KEY` or `AWS_SECRET_KEY`

Alternatively, the credentials can be read from the aws credentials file.

Set `AWS_PROFILE` to specify the correct profile if not "default". 
Set `AWS_SHARED_CREDENTIALS_FILE` if the file is located in a non-standard location.

Finally, set the appropriate AWS region:
`AWS_REGION=`

or set `AWS_SDK_LOAD_CONFIG=true` to use the region in the aws profile.

```shell script
go run . <bucket-name>
```
