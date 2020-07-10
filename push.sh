#!/bin/bash
set -e

AWS_PROFILE="default"
AWS_REGION="us-west-2"
REPO_NAME="query-tee"
REPO_TAG="1.0"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -p|--profile) AWS_PROFILE="$2"; shift ;;
        -r|--region) AWS_REGION="$2"; shift ;;
        -t|--tag) REPO_TAG="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

main () {
    AWS_ACCOUNT_ID=$(get_account_id)
    echo "AWS Account Id: ${AWS_ACCOUNT_ID}"

    ECR=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    echo "ECR: ${ECR}"

    echo "Repo name: ${REPO_NAME}"
    echo "Repo tag: ${REPO_TAG}"

    echo "Setting up ECR repository"
    setup_repository

    echo "Configuring docker credential"
    configure_docker_credential

    echo "Building image"
    build_image

    echo "Pushing image"
    push_image
}

get_account_id() {
    aws sts get-caller-identity --output text --query Account --profile ${AWS_PROFILE} --region ${AWS_REGION}
}

setup_repository() {
    if ! aws ecr describe-repositories --repository-name ${REPO_NAME} --region ${AWS_REGION} --profile ${AWS_PROFILE} 
    then
        aws ecr create-repository --repository-name ${REPO_NAME} --region ${AWS_REGION} --profile ${AWS_PROFILE}
    fi
}

configure_docker_credential() {
    aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin ${ECR}
}

build_image() {
    make
    docker tag quay.io/cortexproject/query-tee:latest ${ECR}/${REPO_NAME}:${REPO_TAG}
}

push_image() {
    docker push ${ECR}/${REPO_NAME}:${REPO_TAG}
}

main "$@"; exit