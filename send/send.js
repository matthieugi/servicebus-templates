const { ServiceBusClient } = require("@azure/service-bus");
const { parentPort, threadId } = require('worker_threads')
require('dotenv').config();

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING;

// name of the queue
const queueName = process.env.SERVICEBUS_QUEUENAME;

 
async function main() {
     //create large batch 
    let messagesBatch = [];

    for (let ligne=1; ligne < 15; ligne++){
        for(let voitureId=1; voitureId < 40; voitureId++){
            messagesBatch = [...messagesBatch, 
            { 
                sessionId: `ligne${threadId ? threadId: '1'}`,
                body: `hello world`,
                subject: `update`
            }
            ];
        }
    }

    // create a Service Bus client using the connection string to the Service Bus namespace
    const sbClient = new ServiceBusClient(connectionString);

    // createSender() can also be used to create a sender for a topic.
    const sender = sbClient.createSender(queueName);
    
    try {
        await sender.sendMessages(messagesBatch);
    }
    catch (e) {
        console.log(e);
    }

    parentPort.postMessage(messagesBatch.length);

    // Close the sender
    await sender.close();
    await sbClient.close();
}

setInterval(main, 500);
