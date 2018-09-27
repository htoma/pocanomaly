using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System;
using System.Configuration;
using System.Net.Http.Headers;

namespace PocAnomaly
{
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

        [FunctionName("clustering")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]
            HttpRequestMessage req,
            [Blob("nyctaxi/sample.csv", FileAccess.Read, Connection = "BlobContainerConnection")] Stream sample,
            TraceWriter log)
        {
            var (status, result) = await ClusterOnMlStudio(sample);
            return req.CreateResponse(status, result);
        }

        static async Task<(HttpStatusCode, string)> ClusterOnMlStudio(Stream sample)
        {
            var points = new List<Dictionary<string, string>>();
            int countLines = 0;

            using (StreamReader reader = new StreamReader(sample, Encoding.UTF8))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    points.Add(
                        new Dictionary<string, string>
                            {
                                {
                                    "data", line.Replace(',', '|')
                                }
                            });
                    ++countLines;
                }
            }

            var scoreRequest = new
            {
                Inputs = new Dictionary<string, List<Dictionary<string, string>>>
                        {
                            {
                                "input1",
                                points
                            },
                        },
                GlobalParameters = new Dictionary<string, string>
                        {
                            {
                                "Range for Number of Centroids",
                                FormatLineCount(countLines)
                            }
                        }
            };

            var response = await Client.PostAsJsonAsync("", scoreRequest);
            return (response.StatusCode, await response.Content.ReadAsStringAsync());

        }

        private static string FormatLineCount(int countLines)
        {
            return
                "%7B%22ParameterType%22%3A%22Integer%22%2C%22useRangeBuilder%22%3Atrue%2C%22literal%22%3A%222%2C%203%2C%204%2C%205%2C%206%2C%207%22%2C%22isLogarithmic%22%3Afalse%2C%22minValue%22%3A2%2C%22maxValue%22%3A"
                + countLines / 2 + "%2C%22count%22%3A%223%22%7D";
        }
    }
}
