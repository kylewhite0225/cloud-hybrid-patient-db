using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using System.Data;
using System.Text.Json;
using System.IO;
using System.Text;

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

        InsuranceData data = ParseJson(s3Event);

        if (data.provider == "" && data.policy == "")
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

    // private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
    // {
    //     context.Logger.LogInformation($"Processed message {message.Body}");
    //
    //     // TODO: Do interesting work based on the new message
    //     await Task.CompletedTask;
    // }
}