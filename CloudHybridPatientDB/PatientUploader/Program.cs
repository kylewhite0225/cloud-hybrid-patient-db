using System.Net;

namespace PatientUploader;
using System;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;


public class PatientUploader
{
    static void Main(string[] args)
    {
        // NO LONGER NEEDED
        // Check if the input arguments array contains the proper number of arguments
        // if (args.Length != 2)
        // {
        //     throw new ArgumentException("Required arguments: file-path data-type");
        // }

        // Capture path from args array
        string path = @args[0];

        // Use FileInfo object to verify if the filepath points to an existing file.
        FileInfo file = new FileInfo(path);
        if (!file.Exists)
        {
            // Throw new exception if the file does not exist.
            throw new FileNotFoundException("File does not exist.");
        }


        // NO LONGER NEEDED
        // Capture file type from args array
        // string type = args[1];

        // Split the path on '.' to get the filetype extension.
        string[] type = path.Split('.');

        // Check if it matches the two accepted filetype (xml).
        if (!String.Equals(type[1], "xml"))
        {
            // Throw an exception if it does not match.
            throw new ArgumentException("File type must be xml.");
        }
        
        // If the arguments pass checks, use UploadFile method
        UploadFile(path, "patientfilesbucket").Wait();
        Console.WriteLine("Done.");
    }

    /// <summary>
    /// Get AWS Credentials by profile name.
    /// </summary>
    /// <param name="profileName">The name of the profile.</param>
    /// <returns>The AWS Credentials</returns>
    /// <exception cref="ArgumentNullException">Profile cannot be null or empty.</exception>
    /// <exception cref="Exception">Profile not found.</exception>
    private static AWSCredentials GetAwsCredentials(string profileName)
    {
        if (String.IsNullOrEmpty(profileName))
        {
            throw new ArgumentNullException("profileName cannot be null or empty.");
        }

        SharedCredentialsFile credFile = new SharedCredentialsFile();
        CredentialProfile profile = credFile.ListProfiles().Find(p => p.Name.Equals(profileName));
        if (profile == null)
        {
            throw new Exception(String.Format("Profile named {0} not found.", profileName));
        }

        return AWSCredentialsFactory.GetAWSCredentials(profile, new SharedCredentialsFile());
    }

    private static async Task UploadFile(string filePath, string bucketName)
    {
        
        // If the arguments pass checks, create credentials file and S3Client objects
        AWSCredentials credentials = GetAwsCredentials("default");

        AmazonS3Client s3Client = new AmazonS3Client(credentials, RegionEndpoint.USEast1);

        try
        {
            PutObjectRequest putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                FilePath = filePath,
            };

            PutObjectResponse response = await s3Client.PutObjectAsync(putRequest);
            Console.WriteLine("File uploading completed.");

            s3Client.Dispose();
            // return Task.CompletedTask;
        }
        catch (AmazonS3Exception e)
        {
            if (e.ErrorCode != null &&
                (e.ErrorCode.Equals("InvalidAccessKeyId")
                 ||
                 e.ErrorCode.Equals("InvalidSecurity")))
            {
                throw new Exception("Check the provided AWS Credentials.");
            }
            else
            {
                throw new Exception("Error occurred: " + e.Message);
            }
        }
    }
}