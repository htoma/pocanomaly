using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Text;
using System.Threading.Tasks;

namespace PocAnomaly
{
    public static class BlobMonitoring
    {
        [FunctionName("BlobMonitoring")]
        public static async Task Run(
            [BlobTrigger("nyctaxi/encoded_chart.pdf", Connection = "BlobContainerConnection")] Stream encodedChartStream,
            [Blob("nyctaxi/chart.pdf", FileAccess.Write, Connection = "BlobContainerConnection")] Stream outputChartStream,
            TraceWriter log)
        {
            byte[] buffer = new byte[encodedChartStream.Length];
            await encodedChartStream.ReadAsync(buffer, 0, (int)encodedChartStream.Length);
            var base64EncodedData = Encoding.UTF8.GetString(buffer);
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            await outputChartStream.WriteAsync(base64EncodedBytes, 0, base64EncodedBytes.Length);
        }
    }
}
