const { ServiceBusClient } = require("@azure/service-bus");
const { parentPort } = require('worker_threads')
require('dotenv').config();

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING;

// name of the queue
const queueName = "nice"

const messages = [
    { body: "Albert Einstein" },
    { body: "Werner Heisenberg" },
    { body: "Marie Curie" },
    { body: "Steven Hawking" },
    { body: "Isaac Newton" },
    { body: "Niels Bohr" },
    { body: "Michael Faraday" },
    { body: "Galileo Galilei" },
    { body: "Johannes Kepler" },
    { body: "Nikolaus Kopernikus" },
    { body: "Albert Einstein" },
    { body: "Werner Heisenberg" },
    { body: "Marie Curie" },
    { body: "Steven Hawking" },
    { body: "Isaac Newton" },
    { body: "Niels Bohr" },
    { body: "Michael Faraday" },
    { body: "Galileo Galilei" },
    { body: "Johannes Kepler" },
    { body: "Nikolaus Kopernikus" },
    { body: "Albert Einstein" },
    { body: "Werner Heisenberg" },
    { body: "Marie Curie" },
    { body: "Steven Hawking" },
    { body: "Isaac Newton" },
    { body: "Niels Bohr" },
    { body: "Michael Faraday" },
    { body: "Galileo Galilei" },
    { body: "Johannes Kepler" },
    { body: "Nikolaus Kopernikus" },
    { body: "Albert Einstein" },
    { body: "Werner Heisenberg" },
    { body: "Marie Curie" },
    { body: "Steven Hawking" },
    { body: "Isaac Newton" },
    { body: "Niels Bohr" },
    { body: "Michael Faraday" },
    { body: "Galileo Galilei" },
    { body: "Johannes Kepler" },
    { body: "Nikolaus Kopernikus" },
 ];

 
 async function main() {
     //create large batch 
     let messagesBatch = [];
     for (let i=0; i < 1000; i++){
         messagesBatch = [...messagesBatch, messages];
     }


     // create a Service Bus client using the connection string to the Service Bus namespace
    const sbClient = new ServiceBusClient(connectionString);

    // createSender() can also be used to create a sender for a topic.
    const sender = sbClient.createSender(queueName);

    try {
        // Tries to send all messages in a single batch.
        // Will fail if the messages cannot fit in a batch.
        // await sender.sendMessages(messages);


        // create a batch object
        let batch = await sender.createMessageBatch(); 
        for (let i = 0; i < messages.length; i++) {
            // for each message in the array            

            // try to add the message to the batch
            if (!batch.tryAddMessage(messages[i])) {            
                // if it fails to add the message to the current batch
                // send the current batch as it is full
                await sender.sendMessages(batch);

                // then, create a new batch 
                batch = await sender.createMessageBatch();

                // now, add the message failed to be added to the previous batch to this batch
                if (!batch.tryAddMessage(messages[i])) {
                    // if it still can't be added to the batch, the message is probably too big to fit in a batch
                    throw new Error("Message too big to fit in a batch");
                }
            }
        }

        // Send the last created batch of messages to the queue
        await sender.sendMessages(batch);

        parentPort.postMessage(batch.count);

        // Close the sender
        await sender.close();
    } finally {
        await sbClient.close();
    }
}

setInterval(main, 10);
