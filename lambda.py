import json
import boto3

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    try:
        # Extract bucket and object details
        print(event)
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        print(f"Triggered by bucket: {bucket_name}, key: {object_key}")
        
        # Start the Glue job
        response = glue_client.start_job_run(
            JobName='sandbox-01-gluejob-etl',
            Arguments={
                '--s3_input_bucket': bucket_name,
                '--s3_input_key': 'input',
                '--s3_output_bucket': bucket_name,
                '--s3_output_prefix': 'processed'
            },
            WorkerType='G.1X',
            NumberOfWorkers=2
        )
        
        job_run_id = response['JobRunId']
        print(f"Started Glue job with JobRunId: {job_run_id}")

        # Polling the Glue job until it finishes
        while True:
            job_status = glue_client.get_job_run(JobName='sandbox-01-gluejob-etl', RunId=job_run_id)
            state = job_status['JobRun']['JobRunState']
            
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                print(f"Glue job finished with state: {state}")
                break
            else:
                print(f"Glue job still running... Current state: {state}")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue Job {job_run_id} completed with state: {state}")
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error occurred: {str(e)}")
        }
