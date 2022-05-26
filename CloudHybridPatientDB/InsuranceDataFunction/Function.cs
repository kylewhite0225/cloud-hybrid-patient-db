using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using System.Data;
using System.Text.Json;
using System.IO;
using System.Text;
using Amazon.SQS;
using Amazon.Runtime;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace InsuranceDataFunction;

public class Function
{
    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {

    }


    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
    /// to respond to SQS messages.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
    {
        string s3Event = evnt.Records[0].Body;

        if (s3Event == null)
        {
            throw new Exception("Event is null.");
        }

        AmazonSQSClient sqsClient = new AmazonSQSClient(GetAwsCredentials(), Amazon.RegionEndpoint.USEast1);
        await sqsClient.DeleteMessageAsync("https://sqs.us-east-1.amazonaws.com/926831757693/upwardQueue", evnt.Records[0].ReceiptHandle);

        InsuranceData data = ParseJson(s3Event);

        if (string.IsNullOrEmpty(data.provider) && string.IsNullOrEmpty(data.policy))
        {
            Console.WriteLine("Patient with ID {0} does not have medical insurance.", data.id);
        } else
        {
            Console.WriteLine("Patient with ID {0}: policy number = {1}, provider = {2}.", data.id, data.policy, data.provider);
        }

        // foreach(var message in evnt.Records)
        // {
        //     await ProcessMessageAsync(message, context);
        // }
    }

    /// <summary>
    /// Parses the JSON file obtained from the s3Event and inserts the data into RDS using Npgsql.
    /// </summary>
    private InsuranceData ParseJson(string json)
    {
        // Console.WriteLine(doc);
        InsuranceData? data = null;
        try
        {
            Console.WriteLine("Serializing JSON");
            // Use JsonSerializerOptions object to allow ignoring case of JSON elements
            JsonSerializerOptions options = new JsonSerializerOptions();
            options.PropertyNameCaseInsensitive = true; // ignore case
            // Use JsonSerializer to deserialize the JSON string into an InsuranceData object
            data = JsonSerializer.Deserialize<InsuranceData?>(json, options);
        }
        catch (Exception e)
        {
            // TODO
            // Does this catch if the JSON is not well formed?
            Console.WriteLine(e.Message);
        }

        if (json == null)
        {
            throw new InvalidOperationException("Event JSON was null.");
        }

        return data;
    }

    private static SessionAWSCredentials GetAwsCredentials()
    {
        return new SessionAWSCredentials(
            "ASIA5PS32JF66C5FU5PG",
            "M2pi4ntusI6/VeQDPba4eYs48dfz0ibhKWfyTdn6",
            "FwoGZXIvYXdzEMf//////////wEaDCCmAyJY6tFETOLeiiLFAUqcnggUUwfYkD14FNqywQOQ1VRupNrFD8jiwcGcs5gpHlcVfpZra+mhtN15L3WJ2LJpAJ2jheQO2e2CoyFuPfd8c/T8+f/eoNJyDfcWadY285mtcQ+EdIVpTT11swGdtKlK2cxlZhig17dGaE8arogQSP8Df9EDXUbxzEk/1SHq7hSWThv00aWwgS2OI/QCqG/M0PQsYXdsbXJL6C7JHTPvK91dsOMW51QonuGjn2P4Qpybg/VJ9XE9X316qOscVDMEvNBCKM+nvJQGMi2dqO2iuT/rWHWZmdFtcdM5MS4x7GqmIxO44WPspB2z5UWuMDNHytz21NnLIbw=");
    }

    // private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
    // {
    //     context.Logger.LogInformation($"Processed message {message.Body}");
    //
    //     // TODO: Do interesting work based on the new message
    //     await Task.CompletedTask;
    // }
}