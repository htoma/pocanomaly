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

namespace PocAnomaly
{
    using System;
    using System.Configuration;
    using System.Net.Http.Headers;

    public static class Clustering
    {
        private static readonly HttpClient Client = GetHttpClient();

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

            return serviceClient.GetContainerReference("mlstudio2");
        }

        static private void AnalyzeResult(ref string[] result)
        {
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
            [Blob("mlstudio2/data.csv", FileAccess.Read, Connection = "BlobContainerConnection")] Stream data,
            TraceWriter log)
        {
            var (status, result) = await ClusterOnMlStudio(data);
            return req.CreateResponse(status, result);
        }

        static async Task<(HttpStatusCode, string)> ClusterOnMlStudio(Stream data)
        {
            int linesCount = 0;
            string input = "data\n";

            using (StreamReader reader = new StreamReader(data, Encoding.UTF8))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (linesCount != 0 && linesCount % 1000 == 0)
                    {
                        Console.WriteLine(linesCount);
                        if (linesCount % 2000 == 0)
                        {
                            break;
                        }
                    }
                    input += line.Replace(',', '|');
                    input += "\n";
                    ++linesCount;
                }
            }

            CloudBlobContainer container = GetContainer();
            CloudBlockBlob inputBlob = container.GetBlockBlobReference("input.csv");
            inputBlob.UploadText(input);

            var scoreRequest = new
            {
                Inputs = new Dictionary<string, List<Dictionary<string, string>>>()
                { },
                GlobalParameters = new Dictionary<string, string>()
                { }
            };

            var response = await Client.PostAsJsonAsync("", scoreRequest);
            string responseContent = "";

            if (response.IsSuccessStatusCode)
            {
                CloudBlockBlob blob = container.GetBlockBlobReference("output.csv");
                using (var memoryStream = new MemoryStream())
                {
                    blob.DownloadToStream(memoryStream);
                    string text = Encoding.ASCII.GetString(memoryStream.ToArray());
                    string[] result = text.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);
                    AnalyzeResult(ref result);
                }
                responseContent = "OK";
            }
            else
            {
                responseContent = await response.Content.ReadAsStringAsync();
            }

            return (response.StatusCode, responseContent);
        }
    }
}
