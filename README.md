# Cloud Computing - Hybrid Patient Database
A cloud/local hybrid architecture for storing/retrieving fictional patient insurance information.

The "Patient Uploader" component of this project is a local utility for uploading XML files to an AWS S3 bucket, which triggers an AWS Lambda
function that places a JSON message containing the patient's ID number in an AWS SQS downward queue. A Windows Service running on a local machine
polls the downward queue for incoming messages, and upon receipt queries a local database file (XML). This service then formulates a
JSON message to be placed in another AWS SQS upward queue, which triggers a second lambda function to display the patient's 
insurance information to AWS Cloudwatch.
