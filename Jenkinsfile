#!/usr/bin/env groovy

pipeline {
    agent { node { label 'DOCKER' } }


    environment {
        hash_commit = "${env.GIT_COMMIT}".substring(0, 7)

        namespace = 'tnt'
        imageName = 'scribedb'
        docker_registry_local_url = 'https://registry-eu-local.subsidia.org'
        docker_credentials_id = 'docker-registry-eu-local'
    }

    stages {

        stage('Initialize') {
            steps {
                script {
                    if (env.BRANCH_NAME == 'master') {
                        image_version = "RELEASE-${new Date().format('yyyyMMdd')}-${hash_commit}"
                    } else {
                        def suffix = env.BRANCH_NAME.replaceAll('/', '-')
                        image_version = "SNAPSHOT-${env.BUILD_NUMBER}-${suffix}"
                    }
                    echo "Image version : ${image_version}"
                }
            }
        }

        
        stage('Build & Push scribedb docker image') {
            steps {
              script{
                  dockerImage = docker.build("${namespace}/${imageName}:${image_version}", " --add-host=nexus.osiris.withoxylane.com:192.168.170.38 .")
              }
              script {
                  docker.withRegistry(docker_registry_local_url, docker_credentials_id) {
                      dockerImage.push() // the image_version tag
                      if (env.BRANCH_NAME == 'master') {
                          dockerImage.push('stable')
                      }
                      dockerImage.push('latest')
                  }
              }
            }
        }

    }
}


