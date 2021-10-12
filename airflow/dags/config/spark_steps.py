

SPARK_STEPS = [
    {
        "Name": "Convert JSON to CSV data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_convert.py"               
            ],
        },
    },
    {
        "Name": "Process data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_process.py"               
            ],
        },
    },
    {
        "Name": "Predict sentiments",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_infer.py"               
            ],
        },
    },
    {
        "Name": "Data quality check",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_quality.py"               
            ],
        },
    },
    {
        "Name": "Ingest data into staging schema",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_stage.py"               
            ],
        },
    },
    {
        "Name": "Ingest data into analytic schema",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "{{params.s3_emr_path}}stwitter_emr.zip",
                "{{params.s3_emr_path}}stwitter_analytic.py"               
            ],
        },
    }       
]