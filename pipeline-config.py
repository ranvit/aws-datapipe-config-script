# TODO: Test the irregular DB/Table mapping capability
# TODO: Test the scheduler params
# TODO: Assert that valid reference-names are being used
# TODO: Find other pitfalls that can be caught prior to pipeline deployment
"""
IDEAS:
Which variables should be #{myVariables}?
Automatic output upload to S3?
  - (since import build config randomly fails when importing from local)
"""

import json


# Definition of the Default Object Node
def default_node(schedType="ONDEMAND"):
    # TODO: different schedType provision
    raw_default = {
        "name": "Default",
        "id": "Default",

        "failureAndRerunMode": "CASCADE",
        "resourceRole": "DataPipelineDefaultResourceRole",
        "role": "DataPipelineDefaultRole",
        "pipelineLogUri": "#{myS3StagingLoc}/logs/",
        "scheduleType": schedType
    }
    if schedType != "ONDEMAND":
        raw_default["schedule"] = {"ref": "DefaultSchedule"}
    return raw_default


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
    return raw_compute


# Definition of scheduler
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-schedule.html
# This should be called conditionally -> if scheduleType != "ONDEMAND"
def scheduler_node(sched_params):
    raw_schedule = {
        "id": "DefaultSchedule",
        "type": "Schedule",

        # Required
        "period": sched_params["period"],

        # Require either of the following BUT NOT BOTH
        "startAt": sched_params["startAt"],
        # "startDateTime": ,

        # Optional
        "name": sched_params["description"]
    }
    return raw_schedule


# Definition for source database
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-jdbcdatabase.html
# Could incorporate multiple databases with flexible user_input form
def source_db(db_params):
    raw_src_db = {
        "id": db_params["db_ref"],
        "type": "JdbcDatabase",

        # Required
        "connectionString": db_params["db_connect"],
        "jdbcDriverClass": "com.{0}.jdbc.Driver".format(db_params["db_type"]),
        "*password": db_params["db_password"],
        "username": db_params["db_username"],

        # Optional
        "name": db_params["db_ref"],
        "jdbcProperties": "allowMultiQueries=true"
    }
    return raw_src_db


# Definition of destination database
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-redshiftdatabase.html
def dest_db(db_param):
    raw_dest_db = {
        "id": db_param["db_ref"],
        "type": "RedshiftDatabase",

        # Required
        "*password": db_param["db_password"],
        "username": db_param["db_username"],

        # Require either of the following BUT NOT BOTH
        # "clusterId": ,
        "connectionString": db_param["db_connect"],

        # Optional
        "name": db_param["db_ref"]
    }
    return raw_dest_db


# Definition of a source sql table
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-sqldatanode.html
def source_table(index, table_params):
    raw_src_table = {
        "id": "SrcTable_{0}".format(index),
        "type": "SqlDataNode",

        # Required
        "table": table_params["table_name"],

        # Optional - but required in this case
        "database": {"ref": table_params["db_name"]},
        "selectQuery": "select * from #{table}",
        "name": "SrcTable_{0}".format(index)
    }
    return raw_src_table


# Definition of an S3 Data Node for Copy Staging
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-s3datanode.html
def s3_staging(index):
    raw_s3_node = {
        "id": "S3StagingDataNode_{0}".format(index),
        "type": "S3DataNode",

        "directoryPath":
            "#{{myS3StagingLoc}}/"
            "#{{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}}/"
            "data/table_{0}".format(index),
        "name": "S3StagingDataNode_{0}".format(index)
    }
    return raw_s3_node


# Definition of a destination redshift table
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-redshiftdatanode.html
def dest_table(index, table_params):
    raw_dest_table = {
        "id": "DestRedshiftTable_{0}".format(index),
        "type": "RedshiftDataNode",

        # Required
        "database": {"ref": table_params["db_name"]},
        "tableName": table_params["table_name"],

        # Optional - but required in this case
        "createTableSql": table_params["create_command"],
        "name": "DestRedshiftTable_{0}".format(index)
    }
    return raw_dest_table


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
    return raw_src_s3_copy


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
    return raw_s3_dest_copy


# Definition for Shell Activity that cleans up S3
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html
# Takes number of jobs as input, waits for every S3->Redshift Copy to complete before cleaning up
def shell_s3_cleanup(num_jobs, sns_bool=0):
    raw_shell_cleanup = {
        "id": "S3StagingCleanupActivity",
        "type": "ShellCommandActivity",

        # Require either of the following BUT NOT BOTH
        "command":
            "(sudo yum -y update aws-cli) && "
            "(aws s3 rm #{myS3StagingLoc}/#{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}/data/ "
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
    if sns_bool == 1:
        raw_shell_cleanup["onSuccess"] = {"ref": "sns-completion-alarm"}

    return raw_shell_cleanup


# Definition for an SNS alarm upon completion of pipeline
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-snsalarm.html
# Called conditionally -> if sns_bool == True
def sns_completion(topic_arn):
    raw_sns = {
        "id": "sns-completion-alarm",
        "type": "SnsAlarm",

        # Required
        "message": "Migration to Redshift finished successfully!! (/^-^)/\n",
        "role": "DataPipelineDefaultRole",
        "subject": "(/^-^)/ Successful Copy to Redshift!",
        "topicArn": topic_arn,

        # Optional
        "name": "sns-completion-alarm"
    }
    return raw_sns


# Builds the configuration for all source databases
def src_db_creater(src_db_list):
    """
    :param src_db_list: List of src_db definitions from user_input
    :return: List of src_db configurations
    """
    return [source_db(db_param) for db_param in src_db_list]


# Builds the configuration for all destination databases
def dest_db_creater(dest_db_list):
    """
    :param dest_db_list: List of dest_db definitions from user_input
    :return: List of dest_db configurations
    """
    return [dest_db(db_param) for db_param in dest_db_list]


# Takes a pair of source-dest tables, and creates corresponding
# SQL/S3/Redshift DataNodes and SQL->S3/S3->Redshift CopyActivities
def table_S3_copy_creater(index, table_pair):
    """
    :param table_pair: A dictionary with two keys - "src_table" and "dest_table"
    :return: A list of [Src_Table, S3 Node, Dest_Table, Src-S3 Copy, S3-Dest Copy]
             Each element of the list is an Object Configuration dictionary
    """
    return [
        source_table(index, table_pair["src_table"]),
        s3_staging(index),
        dest_table(index, table_pair["dest_table"]),
        src_s3_copy(index),
        s3_dest_copy(index)
    ]


# Takes a list of table_pairs, and calls table_S3_copy_creater()
# Iteratively builds list of object configurations
def multiple_table_S3_copy_creater(table_pairs):
    """
    :param table_pairs: List of table_pair dicts whose keys are "src_table" and "dest_table"
    :return: List of ALL SQL/S3/Redshift DataNodes and SQL->S3/S3->Redshift CopyActivities
    """
    ret_list = list()
    for index, table_pair in enumerate(table_pairs):
        ret_list.extend(table_S3_copy_creater(index, table_pair))
    return ret_list


# This function enforces the constraints on number of databases and tables
# To ensure a 1-to-1 mapping between source and destination tables
# Regardless of variability in source/destination databases
def user_input_verifier(user_input):
    assert (user_input["num_jobs"] == len(user_input["table_pairs"]))
    assert (user_input["sns_bool"] in [0, 1])
    assert (user_input["num_src_dbs"] == len(user_input["src_dbs"]))
    assert (user_input["num_dest_dbs"] == len(user_input["dest_dbs"]))
    for key in ["s3_staging_loc", "insert_mode", "security_groups"]:
        assert (key in user_input)


# A builder that constructs the entire configuration
def builder(filepath):
    """
    :param filepath: Path to user_input file
    :return: Pipeline definition dictionary
    """
    with open(filepath) as fp:
        user_input = json.load(fp)
    user_input_verifier(user_input)

    # Definition of outer structure of Pipeline Configuration File.
    raw_cfg = {
        "objects": [
            default_node(user_input["schedule_type"]),
            compute_node()
        ],
        "parameters": list(),
        "values": {
            "myS3StagingLoc": user_input["s3_staging_loc"].rstrip("/"),  # Removing the trailing "/"s
            "myInsertMode": user_input["insert_mode"],
            "mySecurityGrps": user_input["security_groups"]
        }
    }

    # Scheduler Node, if required
    if user_input["schedule_type"] != "ONDEMAND":
        raw_cfg["objects"].append(scheduler_node(user_input["schedule"]))

    # Database, Table, S3, Redshift, Copy Activities
    raw_cfg["objects"].extend(src_db_creater(user_input["src_dbs"]))
    raw_cfg["objects"].extend(dest_db_creater(user_input["dest_dbs"]))
    raw_cfg["objects"].extend(multiple_table_S3_copy_creater(user_input["table_pairs"]))

    # Shell Cleanup
    raw_cfg["objects"].append(shell_s3_cleanup(user_input["num_jobs"], user_input["sns_bool"]))

    # SNS Alarm, if required
    if user_input["sns_bool"] == 1:
        raw_cfg["objects"].append(sns_completion(user_input["sns_topic_arn"]))

    # Write the final pipeline definition dict to a json file
    with open('pipeline-definition-output.json', 'w') as fp:
        json.dump(raw_cfg, fp, indent=2)


if __name__ == "__main__":
    builder('./complex_user_input.json')
