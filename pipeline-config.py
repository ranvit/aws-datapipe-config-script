# TODO: Update "values" dict with all user_input for #{myVariable} types
# TODO: Order definitions by {required/optional} params, incorporate into user_input
# TODO: Pass a dict of user_params to functions?

"""
IDEAS:

Helper functions to replicate database nodes of varying specifications
- Still one-to-one mapping between table migration from source table to dest table
- Only support Redshift as destination_db, or allow flexibility?

"""

# Definition of outer structure of Pipeline Configuration File.
raw_cfg = {
    "objects": list(),
    "parameters": list(),
    "values": dict()
}


# Definition of the Default Object Node
def default_node(schedType="ONDEMAND"):
    # TODO: different schedType provision
    raw_default = {
        "name": "Default",
        "id": "Default",

        "failureAndRerunMode": "CASCADE",
        "resourceRole": "DataPipelineDefaultResourceRole",
        "role": "DataPipelineDefaultRole",
        "pipelineLogUri": "#{myS3StagingLoc}/#{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}/logs/",
        "scheduleType": schedType
    }


# Definition of compute resource
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-ec2resource.html
def compute_node():
    raw_compute = {
        "id": "Ec2Instance",
        "type": "Ec2Resource",

        "name": "Ec2Instance",
        "securityGroups": "#{mySecurityGrps}",
        "terminateAfter": "2 Hours"
    }


# Definition of scheduler
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-schedule.html
# This should be called conditionally -> if scheduleType != "ONDEMAND"
def scheduler_node():
    raw_schedule = {
        "id": "DefaultSchedule",
        "type": "Schedule",

        # Required
        "period": "1 days",

        # Require either of the following BUT NOT BOTH
        "startAt": "FIRST_ACTIVATION_DATE_TIME",
        # "startDateTime": ,

        # Optional
        "name": "Every 1 day"
    }


# Definition for source database
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-jdbcdatabase.html
# Could incorporate multiple databases with flexible user_input form
def source_db(db_type="mysql"):
    raw_src_db = {
        "id": "src_db",
        "type": "JdbcDatabase",

        # Required
        "connectionString": "#{mySrcJdbcConnectStr}",
        "jdbcDriverClass": "com.{0}.jdbc.Driver".format(db_type),
        "*password": "#{*mySrcPassword}",
        "username": "#{mySrcUsername}",

        # Optional
        "name": "src_db",
        "jdbcProperties": "allowMultiQueries=true"
    }


# Definition of destination database
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-redshiftdatabase.html
def dest_db():
    raw_dest_db = {
        "id": "dest_db",
        "type": "RedshiftDatabase",

        # Required
        "*password": "#{*myRedshiftPassword}",
        "username": "#{myRedshiftUsername}",

        # Require either of the following BUT NOT BOTH
        # "clusterId": ,
        "connectionString": "#{myRedshiftJdbcConnectStr}",

        # Optional
        "name": "dest_db"
    }


# Definition of a source sql table
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-sqldatanode.html
def source_table(index):
    raw_src_table = {
        "id": "SrcTable_{0}".format(index),
        "type": "SqlDataNode",

        # Required
        "table": "#{{mySrcTableName_{0}}}".format(index),

        # Optional - but required in this case
        "database": {"ref": "src_db"},
        "selectQuery": "select * from #{table}",
        "name": "SrcTable_{0}".format(index)
    }


# Definition of an S3 Data Node for Copy Staging
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-s3datanode.html
def s3_staging(index):
    raw_s3_node = {
        "id": "S3StagingDataNode_{0}".format(index),
        "type": "S3DataNode",

        "directoryPath":
            "#{{myS3StagingLoc}}/#{{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}}/data/table_{0}".format(index),
        "name": "S3StagingDataNode_{0}".format(index)
    }


# Definition of a destination redshift table
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-redshiftdatanode.html
def dest_table(index, create_command):
    raw_dest_table = {
        "id": "DestRedshiftTable_{0}".format(index),
        "type": "RedshiftDataNode",

        # Required
        "database": {"ref": "dest_db"},
        "tableName": "#{{myRedshiftTableName_{0}}}".format(index),

        # Optional - but required in this case
        "createTableSql": create_command,
        "name": "DestRedshiftTable_{0}".format(index)
    }


# Definition of Copy Activity: SrcTable -> S3
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
def src_s3_copy(index):
    raw_src_s3_copy = {
        "id": "SrcToS3CopyActivity_{0}".format(index),
        "type": "CopyActivity",

        # Require either of the following BUT NOT BOTH
        "runsOn": {"ref": "Ec2Instance"},
        # workerGroup: ,

        # Optional - but required in this case
        "input": {"ref": "SrcTable_{0}".format(index)},
        "output": {"ref": "S3StagingDataNode_{0}".format(index)},
        "name": "RDSToS3CopyActivity_{0}".format(index)
    }


# Definition of Copy Activity: S3 -> DestRedshiftTable
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-redshiftcopyactivity.html
def s3_dest_copy(index):
    raw_s3_dest_copy = {
        "id": "S3ToRedshiftCopyActivity_{0}".format(index),
        "type": "RedshiftCopyActivity",

        # Required
        "insertMode": "#{myInsertMode}",

        # Require either of the following BUT NOT BOTH
        "runsOn": {"ref": "Ec2Instance"},
        # workerGroup: ,

        # Optional - but required in this case
        "input": {"ref": "S3StagingDataNode_{0}".format(index)},
        "output": {"ref": "DestRedshiftTable_{0}".format(index)},
        "name": "S3ToRedshiftCopyActivity_{0}".format(index)
    }


# Definition for Shell Activity that cleans up S3
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html
# Takes number of jobs as input, waits for every S3->Redshift Copy to complete before cleaning up
def shell_s3_cleanup(num_jobs, sns_bool=False):
    raw_shell_cleanup = {
        "id": "S3StagingCleanupActivity",
        "type": "ShellCommandActivity",

        # Require either of the following BUT NOT BOTH
        "command":
            "(sudo yum -y update aws-cli) && " \
            "(aws s3 rm #{myS3StagingLoc}/#{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}/data/ " \
            "--recursive)",
        # "scriptUri": ,

        # Require either of the following BUT NOT BOTH
        "runsOn": {"ref": "Ec2Instance"},
        # workerGroup: ,

        # Optional - but required in this case
        # "input": {"ref": "S3StagingDataNode2"},
        "dependsOn":
            [{"ref": "S3ToRedshiftCopyActivity_{0}".format(i)} for i in range(num_jobs)],
        "stage": "false",
        "name": "S3StagingCleanupActivity"
    }
    if sns_bool:
        raw_shell_cleanup["onSuccess"] = {"ref": "sns-completion-alarm"}


# Definition for an SNS alarm upon completion of pipeline
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-snsalarm.html
# Called conditionally -> if sns_bool == True
def sns_completion(topic_arn):
    raw_sns = {
        "id": "sns-completion-alarm",
        "type": "SnsAlarm",

        # Required
        "message": "Migration to Redshift finished successfully!! (/^▽^)/",
        "role": "DataPipelineDefaultRole",
        "subject": "(/^▽^)/ Copy to Redshift succeeded!!",
        "topicArn": topic_arn,

        # Optional
        "name": "sns-completion-alarm"
    }
