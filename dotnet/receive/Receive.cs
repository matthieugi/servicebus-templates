using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace Receive
{
    public class ReceiveWorker
    {
        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://ironman.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=IUZx7ITn8oiC5VF+7HhcyajmTT4v/U9sumnsBaIF7+s=";

        static int nbMessages = 0;

        // The Service Bus client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when messages are being published or read
        // regularly.
        //
        // the client that owns the connection and can be used to create senders and receivers

        List<ServiceBusProcessor> processors = new List<ServiceBusProcessor>();

        // handle received messages
        Task MessageHandler(ProcessMessageEventArgs args)
        {
            nbMessages++;
            return Task.CompletedTask;
        }

        // handle any errors when receiving messages
        Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        public static async Task<int> ShowStats(){
                var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));

                while (await timer.WaitForNextTickAsync())
                {
                    Console.WriteLine(nbMessages);
                    nbMessages = 0;
                };

                return 0;
        }

        public async Task Run(string topicName, string subscriptionName)
        {
            try
            {
                var sbAdminClient = new ServiceBusAdministrationClient(connectionString);
                ServiceBusClient client = new ServiceBusClient(connectionString);

                var subscriptionOptions = new CreateSubscriptionOptions(topicName, subscriptionName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromMinutes(5),
                    DefaultMessageTimeToLive = TimeSpan.FromSeconds(30),
                    EnableBatchedOperations = true,

                };
                await sbAdminClient.CreateSubscriptionAsync(subscriptionOptions);

                // Thread.Sleep(500);

                for(int i=0; i < 1; i++){
                    // create a processor that we can use to process the messages
                            // the processor that reads and processes messages from the queue
                    ServiceBusProcessor processor = client.CreateProcessor(topicName, subscriptionName, 
                        new ServiceBusProcessorOptions
                        {
                            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,

                            // I can also allow for multi-threading
                            MaxConcurrentCalls = 10,

                            // Set local cache 
                            PrefetchCount = 1200
                        }
                    );

                    // add handler to process messages
                    processor.ProcessMessageAsync += MessageHandler;

                    // add handler to process any errors
                    processor.ProcessErrorAsync += ErrorHandler;

                    // start processing 
                    await processor.StartProcessingAsync();

                    processors.Add(processor);
                }
            }
            finally
            {

            }
        }

    }
}