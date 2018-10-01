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
        class Point
        {
            public Point()
            {
                values = new Dictionary<string, string>();
                distanceToCentroid = 0;
                anomaly = false;
            }

            public string CSVHeader()
            {
                string csv = "";
                foreach (KeyValuePair<string, string> value in values)
                {
                    csv += value.Key + ",";
                }
                csv += "anomaly";
                return csv;
            }
            public string CSVLine()
            {
                string csv = "";
                foreach (KeyValuePair<string, string> value in values)
                {
                    csv += value.Value + ",";
                }
                csv += anomaly ? "true" : "false";
                return csv;
            }

            public Dictionary<string, string> values { get; set; }
            public double distanceToCentroid { get; set; }
            public bool anomaly { get; set; }
        }
        class Cluster
        {
            public Cluster(string id, Point point)
            {
                clusterId = "_" + id + "_";
                elements = new List<Point> { point };
            }

            public string CSVHeader()
            {
                string csv = "";
                if (elements.Count > 0)
                {
                    csv = elements[0].CSVHeader() + "," + "cluster_id\n";
                }
                return csv;
            }

            public string CSVLines()
            {
                string csv = "";
                foreach (Point element in elements)
                {
                    csv += element.CSVLine() + "," + clusterId + "\n";
                }
                return csv;
            }

            public string clusterId { get; set; }
            public List<Point> elements { get; set; }

            public bool anomaly
            {
                set
                {
                    foreach (Point element in elements)
                    {
                        element.anomaly = true;
                    }
                }
            }
        }

        static private Dictionary<string, Cluster> BuildClusterMap(ref string[] result, ref int pointsCount)
        {
            bool firstLine = true;
            int clusterLabelIndex = 0;
            int lineElementsNb = 0;
            List<string> headers = new List<string>();
            pointsCount = 0;
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
                    int clusterId = Convert.ToInt32(split[clusterLabelIndex]);

                    for (int i = 0; i < clusterLabelIndex; ++i)
                    {
                        point.values[headers[i]] = split[i];
                    }
                    point.distanceToCentroid = Convert.ToDouble(split[clusterLabelIndex + 1 + clusterId]);
                    if (map.ContainsKey(split[clusterLabelIndex]))
                    {
                        map[split[clusterLabelIndex]].elements.Add(point);
                    }
                    else
                    {
                        map[split[clusterLabelIndex]] = new Cluster(split[clusterLabelIndex], point);
                    }
                    ++pointsCount;
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

        static private double distanceAverage(Cluster cluster)
        {
            double sum = cluster.elements.Aggregate(new Double(), (acc, point) => acc + point.distanceToCentroid);
            return sum / (double)cluster.elements.Count;
        }

        static private double variance(Cluster cluster, double average)
        {
            double sum = cluster.elements.Aggregate(new Double(), (acc, point) => acc + Math.Pow(point.distanceToCentroid - average, 2));
            return sum / (double)cluster.elements.Count;
        }

        static private double standardDerivation(Cluster cluster, double average)
        {
            return Math.Sqrt(variance(cluster, average));
        }

        static private void foundAnomalies(Cluster cluster)
        {
            double average = distanceAverage(cluster);
            double derivation = standardDerivation(cluster, average);

            foreach (Point point in cluster.elements)
            {
                if (point.distanceToCentroid > average + 2 * derivation || point.distanceToCentroid < average - 2 * derivation)
                {
                    point.anomaly = true;
                }
            };
        }

        static private string BuildCSV(ref Dictionary<string, Cluster> map)
        {
            string csv = "";
            if (map.Count > 0)
            {
                csv = map.ElementAt(0).Value.CSVHeader();
            }
            foreach (KeyValuePair<string, Cluster> cluster in map)
            {
                csv += cluster.Value.CSVLines();
            }
            return csv;
        }

        static private void AnalyzeResult(string text)
        {
            string[] result = text.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);
            int linesCount = result.Length - 1;
            int pointsCount = 0;
            var map = BuildClusterMap(ref result, ref pointsCount);
            double average = (double)pointsCount / (double)map.Count;

            foreach (KeyValuePair<string, Cluster> cluster in map)
            {
                if (((double)cluster.Value.elements.Count / average) < 0.1)
                {
                    cluster.Value.anomaly = true;
                }
                else if (((double)cluster.Value.elements.Count / (double)pointsCount) < 0.05)
                {
                    cluster.Value.anomaly = true;
                }
                else
                {
                    foundAnomalies(cluster.Value);
                }
            }

            CloudBlobContainer container = GetContainer();
            CloudBlockBlob jsonBlob = container.GetBlockBlobReference("result.json");
            string jsonResult = Newtonsoft.Json.JsonConvert.SerializeObject(map.Select(elem => elem.Value).ToList());
            jsonBlob.UploadText(jsonResult);

            CloudBlockBlob csvBlob = container.GetBlockBlobReference("result.csv");
            csvBlob.UploadText(BuildCSV(ref map));
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

                // submit the job
                var response = await client.PostAsJsonAsync(BaseUrl + "?api-version=2.0", request);

                if (!response.IsSuccessStatusCode)
                {
                    return (response.StatusCode, await response.Content.ReadAsStringAsync());
                }

                string jobId = await response.Content.ReadAsAsync<string>();

                // start the job
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
                    response = await client.GetAsync(jobLocation);
                    if (!response.IsSuccessStatusCode)
                    {
                        return (response.StatusCode, await response.Content.ReadAsStringAsync());
                    }

                    BatchScoreStatus status = await response.Content.ReadAsAsync<BatchScoreStatus>();
                    if (watch.ElapsedMilliseconds > TimeOutInMilliseconds)
                    {
                        done = true;
                        await client.DeleteAsync(jobLocation);
                    }
                    switch (status.StatusCode)
                    {
                        case BatchScoreStatusCode.Failed:
                        case BatchScoreStatusCode.Cancelled:
                            return (response.StatusCode, await response.Content.ReadAsStringAsync());
                        case BatchScoreStatusCode.Finished:
                            done = true;
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
                return (done ? HttpStatusCode.OK : HttpStatusCode.RequestTimeout, done ? "OK" : "Timeout");
            }
        }
    }
}
