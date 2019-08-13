# TODO: Update "values" dict with all user_input for #{myVariable} types
# TODO: Order definitions by {required/optional} params, incorporate into user_input
# TODO: Pass a dict of user_params to functions?

raw_cfg = {
    "objects": list(),
    "parameters": list(),
    "values": dict()
}


# Definition of the Default Object Node
def default_node(schedType="ONDEMAND"):
    # TODO: different schedType provision
    raw_default = {
        "failureAndRerunMode": "CASCADE",
        "resourceRole": "DataPipelineDefaultResourceRole",
        "role": "DataPipelineDefaultRole",
        "pipelineLogUri": "#{myS3StagingLoc}/#{format(@scheduledStartTime, 'YYYY-MM-dd-HH-mm-ss')}/logs/",
        "scheduleType": schedType,
        "name": "Default",
        "id": "Default"
    }


# Definition of compute resource
def compute_node():
    raw_compute = {
        "name": "Ec2Instance",
        "securityGroups": "#{mySecurityGrps}",
        "id": "Ec2Instance",
        "type": "Ec2Resource",
        "terminateAfter": "2 Hours"
    }


# Definition of scheduler
# This should be called conditionally -> if scheduleType != "ONDEMAND"
def scheduler_node():
    raw_schedule = {
        "period": "1 days",
        "name": "Every 1 day",
        "id": "DefaultSchedule",
        "type": "Schedule",
        "startAt": "FIRST_ACTIVATION_DATE_TIME"
    }


# Definition for source database
# Could incorporate multiple databases with flexible user_input form
def source_db():
    raw_src_db = {
        "id": "src_db",
        "type": "JdbcDatabase",

        # Required
        "connectionString": "#{myRDSJdbcConnectStr}",
        "jdbcDriverClass": "com.mysql.jdbc.Driver",  # Multiple configs - mysql, psql,
        "*password": "#{*myRDSPassword}",
        "username": "#{myRDSUsername}",

        # Optional
        "name": "src_db",
        "jdbcProperties": "allowMultiQueries=true"
    }
