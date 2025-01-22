#!/bin/bash

S3_BUCKET="assignment2-emr"

AWS_REGION="us-east-1"

directories=(
    "jars"
    "logs"
    "input"
    "output"
)

sub_modules=(
    "count-N-cw1w2"
    "count-cw1"
    "count-cw2"
    "calculate-npmi"
    "filter-npmi"
    "sort-npmi"
    "top-n"
)

# Function to build Maven project in the current directory
build_maven() {
    echo "Building Maven project in directory: $(pwd)"
    mvn clean install
}

# Function to create S3 bucket if it doesn't exist
create_s3_bucket() {
    local s3_bucket=$1
    local region=$2

    echo "Checking if S3 bucket $s3_bucket exists..."
    if ! aws s3 ls "s3://$s3_bucket" 2>&1 | grep -q 'NoSuchBucket'; then
        echo "S3 bucket $s3_bucket already exists."
    else
        echo "Creating S3 bucket $s3_bucket in region $region..."
        aws s3 mb "s3://$s3_bucket" --region "$region"
    fi
}

# Function to upload JAR files to S3 bucket
upload_to_s3() {
    local source_file=$1
    local s3_bucket=$2
    local destination_path=$3

    local file_name=$(basename "$source_file")
    echo "Uploading $source_file to S3 bucket $s3_bucket at $destination_path"
    aws s3 cp "$source_file" "s3://$s3_bucket/$destination_path/$file_name"
}

# Function to create directories in S3 bucket if they don't exist
create_s3_directory() {
    local s3_bucket=$1
    local destination_path=$2

    echo "Creating directory in S3 bucket $s3_bucket at $destination_path if it doesn't exist..."
    aws s3api put-object --bucket "$s3_bucket" --key "$destination_path/" --region "$AWS_REGION" &>/dev/null
}

# Create S3 bucket if it doesn't exist
create_s3_bucket "$S3_BUCKET" "$AWS_REGION"

# Create directories in S3 bucket if they don't exist
for directory in "${directories[@]}"; do
    create_s3_directory "$S3_BUCKET" "$directory"
done

# Build Maven project
if ! build_maven; then
    echo "Failed to build project, Aborting."
    exit 1
else
    echo "Project built successfully."
fi

# Upload JAR files to S3 bucket
for module in "${sub_modules[@]}"; do
    for jar_file in "$module"/target/*.jar; do
        if [ -f "$jar_file" ]; then
            upload_to_s3 "$jar_file" "$S3_BUCKET" "jars"
        fi
    done
done