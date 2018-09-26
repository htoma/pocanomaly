using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace PocAnomaly
{
    public static class BlobMonitoring
    {
        [FunctionName("BlobMonitoring")]
        public static void Run(
            [BlobTrigger("nyctaxi/{name}", Connection = "BlobContainerConnection")]
            Stream myBlob,
            string name,
            TraceWriter log)
        {
            // note(horia): at this point the blob {name} was upserted
            log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
        }
    }
}
