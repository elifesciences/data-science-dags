elifePipeline {
    node('containers-jenkins-plugin') {
        def commit
        def jenkins_image_building_ci_pipeline = 'process/process-data-hub-airflow-image-update-repo-list'
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

        elifeMainlineOnly {
            stage 'Merge to master', {
                elifeGitMoveToBranch commit, 'master'
            }
            stage 'Push unstable image', {
                def image = DockerImage.elifesciences(this, 'data-science-dags_peerscout-api', commit)
                def unstable_image = image.addSuffixAndTag('_unstable', commit)
                unstable_image.tag('latest').push()
                unstable_image.tag("${branch}-${commitShort}-${timestamp}").push()
                unstable_image.push()
            }
            stage 'Build data pipeline image with latest commit', {
                triggerImageBuild(jenkins_image_building_ci_pipeline, git_url, commit)
            }
        }

        elifeTagOnly { tagName ->
            def candidateVersion = tagName - "v"

            stage 'Push release image', {
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
