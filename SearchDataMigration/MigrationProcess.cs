using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Zenfolio.Listing.Contracts.Enums;
using Zenfolio.Listing.Contracts.Events;

namespace SearchDataMigration
{
    public class MigrationProcess
    {
        private readonly IConfigurationRoot _configuration;

        public MigrationProcess()
        {
            IConfigurationBuilder builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            _configuration = builder.Build();
        }

        public async Task ProcessAsync()
        {
            Console.WriteLine("Starting process");

            var photographersExternalIds = new List<Guid>();
            using (var r = new StreamReader("photographers.json"))
            {
                string json = r.ReadToEnd();
                photographersExternalIds = JsonConvert.DeserializeObject<List<Guid>>(json);
            }

            string sbConnectionString = _configuration.GetSection("ServiceBus").GetSection("ConnectionString").Value;
            string listingTopic = "listingtopic";

            var topicClient = new TopicClient(sbConnectionString, listingTopic);

            var contractResolver = new DefaultContractResolver
            {
                NamingStrategy = new CamelCaseNamingStrategy()
            };

            var jsonSerializerSettings = new JsonSerializerSettings
            {
                ContractResolver = contractResolver,
                Formatting = Formatting.Indented
            };

            int count = 0;
            int wait = 200;

            foreach (Guid photographerExternalId in photographersExternalIds)
            {
                Console.WriteLine($"Sending to Service Bus -> 'ExternalCalendarConnected' event for Photographer User Id '{photographerExternalId.ToString()}'");
                var externalCalendarConnectedEvent = new ExternalCalendarConnected(photographerExternalId, CalendarProvider.Google);
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(externalCalendarConnectedEvent, jsonSerializerSettings)))
                {
                    MessageId = Guid.NewGuid().ToString(),
                    Label = "ExternalCalendarConnected"
                };
                try
                {
                    await topicClient.SendAsync(message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                count++;
                if (count % 200 == 0)
                {
                    Console.WriteLine($"!!!! WAITING {wait} ms..................... !!!");
                    await Task.Delay(wait);
                }
            }

            Console.WriteLine("Process completed");
        }
    }
}
