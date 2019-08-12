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

