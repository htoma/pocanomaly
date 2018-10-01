using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Text;
using System.Threading.Tasks;

namespace PocAnomaly
{
    public static class BlobMonitoring3
    {
        [FunctionName("BlobMonitoring3")]
        public static async Task Run(
            [BlobTrigger("mlstudio3/encoded3.txt", Connection = "BlobContainerConnection")] Stream encodedChartStream,
            [Blob("mlstudio3/chart3.pdf", FileAccess.Write, Connection = "BlobContainerConnection")] Stream outputChartStream,
            TraceWriter log)
        {
            byte[] buffer = new byte[encodedChartStream.Length];
            await encodedChartStream.ReadAsync(buffer, 0, (int)encodedChartStream.Length);
            string[] data = Encoding.UTF8.GetString(buffer).Split(new string[] { Environment.NewLine }, StringSplitOptions.None);
            var base64EncodedData = data[1];
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            await outputChartStream.WriteAsync(base64EncodedBytes, 0, base64EncodedBytes.Length);
        }
    }
}
