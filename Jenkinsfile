elifePipeline {
    node('containers-jenkins-plugin') {
        def image_repo = 'elifesciences/data-science-dags'
        def jenkins_image_building_ci_pipeline = 'process/process-data-hub-airflow-image-update-repo-list'

        def commit
        def commitShort
        def branch
        def timestamp
        def git_url

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
            commitShort = elifeGitRevision().substring(0, 8)
            branch = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()
            timestamp = sh(script: 'date --utc +%Y%m%d.%H%M', returnStdout: true).trim()
            git_url = getGitUrl()
        }

        stage 'Build and run tests', {
            // Note: we are using staging source dataset in ci
            //   because some source tables or views may not be present in ci
            withDataPipelineGcpCredentials {
                try {
                    timeout(time: 30, unit: 'MINUTES') {
                        sh "make IMAGE_TAG=${commit} REVISION=${commit} \
                            DATA_SCIENCE_SOURCE_DATASET=staging \
                            DATA_SCIENCE_OUTPUT_DATASET=ci \
                            ci-build-and-test"
                    }
                } finally {
                    sh "docker-compose logs"
                    sh "make ci-clean"
                }
            }
        }

        stage 'Build main image', {
            sh "make IMAGE_REPO=${image_repo} IMAGE_TAG=${commit} ci-build-main-image"
        }

        elifeMainlineOnly {
            def dev_image_repo = image_repo + '_unstable'

            stage 'Push image', {
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=${commit} IMAGE_REPO=${dev_image_repo} retag-push-image"
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=${branch}-${commitShort}-${timestamp} IMAGE_REPO=${dev_image_repo} retag-push-image"
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=latest IMAGE_REPO=${dev_image_repo} retag-push-image"
            }

            stage 'Push unstable image for PeerScout API', {
                def image = DockerImage.elifesciences(this, 'data-science-dags_peerscout-api', commit)
                def unstable_image = image.addSuffixAndTag('_unstable', commit)
                unstable_image.tag('latest').push()
                unstable_image.tag("${branch}-${commitShort}-${timestamp}").push()
                unstable_image.push()
            }

            stage 'Merge to master', {
                elifeGitMoveToBranch commit, 'master'
            }
        }

        elifeTagOnly { tagName ->
            def candidateVersion = tagName - "v"

            stage 'Push release image', {
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=latest IMAGE_REPO=${image_repo} retag-push-image"
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=${candidateVersion} IMAGE_REPO=${image_repo} retag-push-image"
            }

            stage 'Push release image for PeerScout API', {
                def image = DockerImage.elifesciences(this, 'data-science-dags_peerscout-api', commit)
                image.tag('latest').push()
                image.tag(candidateVersion).push()
            }
        }

    }
}

def withDataPipelineGcpCredentials(doSomething) {
    try {
        sh 'vault.sh kv get -field credentials secret/containers/data-pipeline/gcp > credentials.json'
        doSomething()
    } finally {
        sh 'echo > credentials.json'
    }
}

def triggerImageBuild(jenkins_image_building_ci_pipeline, gitUrl, gitCommitRef){
    build job: jenkins_image_building_ci_pipeline,  wait: false, parameters: [string(name: 'gitUrl', value: gitUrl), string(name: 'gitCommitRef', value: gitCommitRef)]
}

def getGitUrl() {
    return sh(script: "git config --get remote.origin.url", returnStdout: true).trim()
}
