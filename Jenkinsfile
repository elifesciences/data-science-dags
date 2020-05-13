elifePipeline {
    node('containers-jenkins-plugin') {
        def commit
        def jenkins_image_building_ci_pipeline = 'process/process-data-hub-airflow-image-update-repo-list'
        def git_url

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
            git_url = getGitUrl()
        }

        stage 'Build and run tests', {
            // Note: we are using staging source dataset in ci
            //   because some source tables or views may not be present in ci
            try {
                sh "make IMAGE_TAG=${commit} REVISION=${commit} \
                    DATA_SCIENCE_SOURCE_DATASET=staging \
                    DATA_SCIENCE_OUTPUT_DATASET=ci \
                    ci-build-and-test"
            } finally {
                sh "make ci-clean"
            }
        }

        elifeMainlineOnly {
            stage 'Merge to master', {
                elifeGitMoveToBranch commit, 'master'
            }
            stage 'Build data pipeline image with latest commit', {
                triggerImageBuild(jenkins_image_building_ci_pipeline, git_url, commit)
            }
        }
    }
}


def triggerImageBuild(jenkins_image_building_ci_pipeline, gitUrl, gitCommitRef){
    build job: jenkins_image_building_ci_pipeline,  wait: false, parameters: [string(name: 'gitUrl', value: gitUrl), string(name: 'gitCommitRef', value: gitCommitRef)]
}

def getGitUrl() {
    return sh(script: "git config --get remote.origin.url", returnStdout: true).trim()
}
