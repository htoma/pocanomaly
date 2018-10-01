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

    class Charts
    {
        public static async Task InvokeRequestResponseService()
        {
            using (var client = new HttpClient())
            {
                var scoreRequest = new
                {
                    Inputs = new Dictionary<string, List<Dictionary<string, string>>>()
                    {
                    },
                    GlobalParameters = new Dictionary<string, string>()
                    {
                    }
                };

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", ConfigurationManager.AppSettings["MlStudioChartsApiKey"]);
                client.BaseAddress = new Uri(ConfigurationManager.AppSettings["MlStudioChartsApiUrl"]);
                await client.PostAsJsonAsync("", scoreRequest);
            }
        }
    }


    public static class BlobResultMonitoring
    {
        [FunctionName("BlobResultMonitoring")]
        public static async Task Run(
            [BlobTrigger("mlstudio3/result.csv", Connection = "BlobContainerConnection")] Stream encodedChartStream,
            TraceWriter log)
        {
            await Charts.InvokeRequestResponseService();
        }
    }
}