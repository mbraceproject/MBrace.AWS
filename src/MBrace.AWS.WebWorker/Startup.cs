using System;
using System.Threading.Tasks;
using Microsoft.Owin;
using Owin;
using MBrace.AWS;
using MBrace.AWS.Service;

[assembly: OwinStartup(typeof(MBrace.AWS.WebWorker.Startup))]

namespace MBrace.AWS.WebWorker
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            var settings = MBrace.AWS.WebWorker.Properties.Settings.Default;
            var hostname = $"webWorker-{Environment.MachineName}";
            var creds = new MBraceAWSCredentials(settings.AccessKey, settings.SecretKey);
            var region = AWSRegion.Parse(settings.Region);
            var config = new Configuration(AWSRegion.EUCentral1, creds);
            var service = new WorkerService(config, hostname);

            service.Start();
        }
    }
}
