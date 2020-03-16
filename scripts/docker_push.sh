#!/bin/bash

echo 'Docker Login - BEGIN'
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
echo 'Docker Login - END'

echo 'Docker Push - BEGIN'
docker push decathlon/scribedb:$TRAVIS_TAG
echo 'Docker Push - END'