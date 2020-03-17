#!/bin/bash

echo 'Docker Login - BEGIN'
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
echo 'Docker Login - END'

echo 'Docker Push - BEGIN'
docker push ossdecathlon/scribedb:$TAG
echo 'Docker Push - END'