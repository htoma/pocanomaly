using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Diagnostics;
using System.Threading;

namespace PocAnomaly
{
    using System;
    using System.Configuration;
    using System.Net.Http.Headers;

    public class AzureBlobDataReference
    {
        // Storage connection string used for regular blobs. It has the following format:
        // DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY
        // It's not used for shared access signature blobs.
        public string ConnectionString { get; set; }

        // Relative uri for the blob, used for regular blobs as well as shared access
        // signature blobs.
        public string RelativeLocation { get; set; }

        // Base url, only used for shared access signature blobs.
        public string BaseLocation { get; set; }

        // Shared access signature, only used for shared access signature blobs.
        public string SasBlobToken { get; set; }
    }

    public enum BatchScoreStatusCode
    {
        NotStarted,
        Running,
        Failed,
        Cancelled,
        Finished
    }

    public class BatchScoreStatus
    {
        // Status code for the batch scoring job
        public BatchScoreStatusCode StatusCode { get; set; }

        // Locations for the potential multiple batch scoring outputs
        public IDictionary<string, AzureBlobDataReference> Results { get; set; }

        // Error details, if any
        public string Details { get; set; }
    }

    public class BatchExecutionRequest
    {
        public IDictionary<string, AzureBlobDataReference> Inputs { get; set; }

        public IDictionary<string, string> GlobalParameters { get; set; }

        // Locations for the potential multiple batch scoring outputs
        public IDictionary<string, AzureBlobDataReference> Outputs { get; set; }
    }


    public static class Clustering
    {
        private static HttpClient GetHttpClient()
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Bearer",
                ConfigurationManager.AppSettings["MlStudioApiKey"]);
            client.BaseAddress = new Uri(ConfigurationManager.AppSettings["MlStudioApiUrl"]);
            return client;
        }

        class Point
        {
            public Point()
            {
                values = new Dictionary<string, string>();
                distanceToCentroides = new Dictionary<string, double>();
            }
            public Dictionary<string, string> values { get; set; }
            public Dictionary<string, double> distanceToCentroides { get; set; }
        }
        class Cluster
        {
            public Cluster(string id, Point point)
            {
                clusterId = id;
                anomaly = false;
                elements = new List<Point> { point };
            }

            public string clusterId { get; set; }
            public bool anomaly { get; set; }
            public List<Point> elements { get; set; }
        }

        static private Dictionary<string, Cluster> BuildClusterMap(ref string[] result, ref int biggestCluster)
        {
            bool firstLine = true;
            int clusterLabelIndex = 0;
            int lineElementsNb = 0;
            List<string> headers = new List<string>();

            var map = new Dictionary<string, Cluster>();
            foreach (string line in result)
            {
                List<string> split = line.Split(new Char[] { ',' }).ToList<string>();

                if (firstLine)
                {
                    clusterLabelIndex = split.IndexOf("Cluster Label");
                    lineElementsNb = split.Count;
                    headers = split;
                    firstLine = false;
                }
                else if (lineElementsNb == split.Count)
                {
                    var point = new Point();
                    for (int i = 0; i < clusterLabelIndex; ++i)
                    {
                        point.values[headers[i]] = split[i];
                    }
                    for (int i = clusterLabelIndex + 1; i < lineElementsNb; ++i)
                    {
                        point.distanceToCentroides[headers[i]] = Convert.ToDouble(split[i]);
                    }
                    if (map.ContainsKey(split[clusterLabelIndex]))
                    {
                        map[split[clusterLabelIndex]].elements.Add(point);
                    }
                    else
                    {
                        map[split[clusterLabelIndex]] = new Cluster(split[clusterLabelIndex], point);
                    }
                    biggestCluster = Math.Max(biggestCluster, map[split[clusterLabelIndex]].elements.Count);
                }
            }

            return map;
        }
        static private CloudBlobContainer GetContainer()
        {
            string storageConnectionString = "DefaultEndpointsProtocol=https;"
                   + "AccountName=horiatesta52b"
                   + ";AccountKey=1FSwKfUdi5YdYDyzNax4z/lh1aTS5GUEfzog4XWngOH/+SLtTW2MTRlfsGfDoi9UjhULIH4UdRGYrD8C49r2lw=="
                   + ";EndpointSuffix=core.windows.net";

            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient serviceClient = account.CreateCloudBlobClient();

            return serviceClient.GetContainerReference("mlstudio3");
        }

        static private void AnalyzeResult(string text)
        {
            string[] result = text.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);
            int linesCount = result.Length - 1;
            int biggestCluster = 0;
            var map = BuildClusterMap(ref result, ref biggestCluster);

            string output = "";
            foreach (KeyValuePair<string, Cluster> points in map)
            {
                Cluster cluster = points.Value;
                if (((double)cluster.elements.Count / (double)biggestCluster) < 0.1)
                {
                    cluster.anomaly = true;
                }
                double repartition = (double)cluster.elements.Count / (double)linesCount;
                output += "Cluster : " + points.Key + ", repartition : "
                        + repartition + " anomaly : " + (cluster.anomaly ? "yes" : "no") + "\n";
            }
            
            string jsonResult = Newtonsoft.Json.JsonConvert.SerializeObject(map.Select(elem => elem.Value).ToList());
            CloudBlobContainer container = GetContainer();
            CloudBlockBlob inputBlob = container.GetBlockBlobReference("result.json");
            inputBlob.UploadText(jsonResult);
        }

        [FunctionName("clustering")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]
            HttpRequestMessage req,
            TraceWriter log)
        {
            string inputBlob = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "blob", true) == 0)
                .Value;

            var (status, result) = await ClusterOnMlStudio(inputBlob);
            return req.CreateResponse(status, result);
        }

        static async Task<(HttpStatusCode, string)> ClusterOnMlStudio(string inputBlob)
        {
            CloudBlobContainer container = GetContainer();

            string BaseUrl = ConfigurationManager.AppSettings["MlStudioApiUrl"];
            string apiKey = ConfigurationManager.AppSettings["MlStudioApiKey"];

            const int TimeOutInMilliseconds = 3600 * 1000; // Set a timeout of 1h

            using (HttpClient client = new HttpClient())
            {
                var request = new BatchExecutionRequest()
                {
                    GlobalParameters = new Dictionary<string, string>()
                    {
                       {
                            "Path to container, directory or blob", inputBlob
                       },
                    }
                };

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

                Console.WriteLine("Submitting the job...");

                // submit the job
                var response = await client.PostAsJsonAsync(BaseUrl + "?api-version=2.0", request);

                if (!response.IsSuccessStatusCode)
                {
                    return (response.StatusCode, await response.Content.ReadAsStringAsync());
                }

                string jobId = await response.Content.ReadAsAsync<string>();
                Console.WriteLine(string.Format("Job ID: {0}", jobId));

                // start the job
                Console.WriteLine("Starting the job...");
                response = await client.PostAsync(BaseUrl + "/" + jobId + "/start?api-version=2.0", null);
                if (!response.IsSuccessStatusCode)
                {
                    return (response.StatusCode, await response.Content.ReadAsStringAsync());
                }

                string jobLocation = BaseUrl + "/" + jobId + "?api-version=2.0";
                Stopwatch watch = Stopwatch.StartNew();
                bool done = false;
                while (!done)
                {
                    Console.WriteLine("Checking the job status...");
                    response = await client.GetAsync(jobLocation);
                    if (!response.IsSuccessStatusCode)
                    {
                        return (response.StatusCode, await response.Content.ReadAsStringAsync());
                    }

                    BatchScoreStatus status = await response.Content.ReadAsAsync<BatchScoreStatus>();
                    if (watch.ElapsedMilliseconds > TimeOutInMilliseconds)
                    {
                        done = true;
                        Console.WriteLine(string.Format("Timed out. Deleting job {0} ...", jobId));
                        await client.DeleteAsync(jobLocation);
                    }
                    switch (status.StatusCode)
                    {
                        case BatchScoreStatusCode.NotStarted:
                            Console.WriteLine(string.Format("Job {0} not yet started...", jobId));
                            break;
                        case BatchScoreStatusCode.Running:
                            Console.WriteLine(string.Format("Job {0} running...", jobId));
                            break;
                        case BatchScoreStatusCode.Failed:
                            Console.WriteLine(string.Format("Job {0} failed!", jobId));
                            Console.WriteLine(string.Format("Error details: {0}", status.Details));
                            return (response.StatusCode, await response.Content.ReadAsStringAsync());
                        case BatchScoreStatusCode.Cancelled:
                            Console.WriteLine(string.Format("Job {0} cancelled!", jobId));
                            return (response.StatusCode, await response.Content.ReadAsStringAsync());
                        case BatchScoreStatusCode.Finished:
                            done = true;
                            Console.WriteLine(string.Format("Job {0} finished!", jobId));
                            Console.WriteLine("Done ");
                            CloudBlockBlob blob = container.GetBlockBlobReference("output.csv");
                            using (var memoryStream = new MemoryStream())
                            {
                                blob.DownloadToStream(memoryStream);
                                AnalyzeResult(Encoding.ASCII.GetString(memoryStream.ToArray()));
                            }
                            break;
                    }

                    if (!done)
                    {
                        Thread.Sleep(1000); // Wait one second
                    }
                }
                return (HttpStatusCode.OK, "OK");
            }
        }
    }
}
