provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "pdk-test-bucket" {
  bucket = "pdk-test-bucket"
  acl    = "private"

  tags {
    Name        = "PDK test bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_object" "object1" {
  bucket = "pdk-test-bucket"
  depends_on = ["aws_s3_bucket.pdk-test-bucket"]
  key    = "myfile1"
  source = "testdata/myfile1"
  etag   = "${md5(file("testdata/myfile1"))}"
}

resource "aws_s3_bucket_object" "object2" {
  bucket = "pdk-test-bucket"
  depends_on = ["aws_s3_bucket.pdk-test-bucket"]
  key    = "myfile2"
  source = "testdata/myfile2"
  etag   = "${md5(file("testdata/myfile2"))}"
}