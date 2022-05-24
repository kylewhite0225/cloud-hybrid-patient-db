using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;
using Amazon.SQS;
using System.Text;
using System.Text.Json;
using System.Xml.Serialization;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace PatientReaderFunction;

public class Function
{
    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client"></param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }
    
    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task<string?> FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var s3Event = evnt.Records?[0].S3;
        if (s3Event == null)
        {
            throw new Exception("Event is null");
        }

        string bucketName = s3Event.Bucket.Name;
        string objectKey = s3Event.Object.Key;

        Console.WriteLine("Bucket and object key: {0}, {1}", bucketName, objectKey);

        try
        {
            // Create stream to get object from S3
            Console.WriteLine("Create stream object with GetObjectStreamAsync");
            Stream stream = await S3Client.GetObjectStreamAsync(bucketName, objectKey, null);
            Console.WriteLine("Created stream.");

            // Create jsonString string which will contain the contents of the file itself
            string fileContent;

            Console.WriteLine("Use StreamReader");
            // Using StreamReader and the previously created stream
            using (StreamReader reader = new StreamReader(stream))
            {
                // Populate the jsonString string
                fileContent = reader.ReadToEnd();
                // Close reader
                reader.Close();
            }

            Console.WriteLine("Stream reader completed populating file content.");

            // Parse the patient xml file.
            XmlRootAttribute xRoot = new XmlRootAttribute();
            xRoot.ElementName = "patient";
            xRoot.IsNullable = true;
            XmlSerializer serializer = new XmlSerializer(typeof(Patient), xRoot);
            var parsed = serializer.Deserialize(new MemoryStream(ASCIIEncoding.UTF8.GetBytes(fileContent)));
            Patient patient = (Patient)parsed;

            if (patient == null)
            {
                throw new Exception("Failed to parse patient xml.");
            }

            Console.WriteLine("Successfully parsed patient xml.");

            Console.WriteLine("Serializing into json message.");
            string jsonMessage = JsonSerializer.Serialize<Patient>(patient);

            //Console.WriteLine("Attempting to send json message to queue");
            //var sqsClient = new AmazonSQSClient();
            //sqsClient.SendMessageAsync(
            //    "https://sqs.us-east-1.amazonaws.com/926831757693/downwardQueue",
            //    jsonMessage).Wait();

            //Console.WriteLine("Successfully sent message to queue.");

            return jsonMessage;
        }
        catch(Exception e)
        {
            context.Logger.LogInformation($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
            context.Logger.LogInformation(e.Message);
            context.Logger.LogInformation(e.StackTrace);
            throw;
        }
    }
}