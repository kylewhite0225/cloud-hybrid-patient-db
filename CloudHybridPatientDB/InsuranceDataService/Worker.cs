using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.IO;
using Amazon.SQS;
using PatientReaderFunction;
using System.Text.Json;
using System.Xml.Linq;
using Amazon.Runtime.CredentialManagement;
using Amazon.Runtime;
using Amazon.SQS.Model;

namespace InsuranceDataService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;


        private const string logPath = @"C:\Temp\InsuranceData.log";

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            //Do here anything you want to do when the service starts

            await base.StartAsync(cancellationToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            //Do here anything you want to do when the service stops

            await base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                WriteToLog("\n\nBeginning...");
                AmazonSQSClient sqsClient;
                try
                {
                    sqsClient = new AmazonSQSClient(GetAwsCredentials(), Amazon.RegionEndpoint.USEast1);
                
                    WriteToLog("Receiving SQS Message from downward queue...");
                    var response = await sqsClient.ReceiveMessageAsync(
                        "https://sqs.us-east-1.amazonaws.com/926831757693/downwardQueue"
                        );
                    WriteToLog("Received message.");

                    string dbPath = "C:\\Users\\jessi\\OneDrive\\Documents\\School\\CS455\\cloud-hybrid-patient-db\\InsuranceDatabase\\InsuranceDatabase.xml";
                    XDocument dbDoc = XDocument.Load(dbPath);
                    WriteToLog("Loaded xml database");

                    foreach (var message in response.Messages)
                    {
                        WriteToLog("message: " + message.Body);
                        Patient? patient = JsonSerializer.Deserialize<Patient>(message.Body);
                        if (patient == null)
                        {
                            throw new Exception("Could not deserialize queue message into Patient object");
                        }
                        WriteToLog("Deserialized patient successfully");

                        var element = dbDoc.Descendants()
                            .Where(x => (string)x.Attribute("id") == patient.id.ToString())
                            .FirstOrDefault();
                        WriteToLog("Attempted to find patient in db.");

                        PatientInsurance insurance = new PatientInsurance();
                        insurance.id = patient.id;
                        if (element != null)
                        {
                            WriteToLog("Found patient in db.  Attempting to get insurance fields");
                            // Found insurance policy, populate insurance related fields
                            var policyElement = element.Descendants().Where(x => x.Attribute("policyNumber") != null).FirstOrDefault();
                            insurance.policy = policyElement.Attribute("policyNumber").Value;

                            var providerElement = policyElement.Descendants().FirstOrDefault();
                            insurance.provider = providerElement.Value;
                        }

                        WriteToLog("Deleting message from queue...");
                        await sqsClient.DeleteMessageAsync("https://sqs.us-east-1.amazonaws.com/926831757693/downwardQueue", message.ReceiptHandle);

                        // Send insurance info to upward queue
                        WriteToLog("Sending message to queue...");
                        string jsonMessage = JsonSerializer.Serialize<PatientInsurance>(insurance);
                        await sqsClient.SendMessageAsync(
                            "https://sqs.us-east-1.amazonaws.com/926831757693/upwardQueue",
                            jsonMessage);
                        WriteToLog("Successfully sent message.");
                    }
                }
                catch (Exception e)
                {
                    WriteToLog(e.Message);
                    throw e;
                }

                await Task.Delay(5000, stoppingToken);
            }
        }

        public void WriteToLog(string message)
        {
            string text = String.Format("{0}:\t{1}", DateTime.Now, message);
            using (StreamWriter writer = new StreamWriter(logPath, append: true))
            {
                writer.WriteLine(text);
            }
        }

        private static SessionAWSCredentials GetAwsCredentials()
        {
            return new SessionAWSCredentials(
                "ASIA5PS32JF66C5FU5PG",
                "M2pi4ntusI6/VeQDPba4eYs48dfz0ibhKWfyTdn6",
                "FwoGZXIvYXdzEMf//////////wEaDCCmAyJY6tFETOLeiiLFAUqcnggUUwfYkD14FNqywQOQ1VRupNrFD8jiwcGcs5gpHlcVfpZra+mhtN15L3WJ2LJpAJ2jheQO2e2CoyFuPfd8c/T8+f/eoNJyDfcWadY285mtcQ+EdIVpTT11swGdtKlK2cxlZhig17dGaE8arogQSP8Df9EDXUbxzEk/1SHq7hSWThv00aWwgS2OI/QCqG/M0PQsYXdsbXJL6C7JHTPvK91dsOMW51QonuGjn2P4Qpybg/VJ9XE9X316qOscVDMEvNBCKM+nvJQGMi2dqO2iuT/rWHWZmdFtcdM5MS4x7GqmIxO44WPspB2z5UWuMDNHytz21NnLIbw=");
        }
    }
}