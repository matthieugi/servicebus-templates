const { EventHubProducerClient } = require("@azure/event-hubs");
const { DefaultAzureCredential } = require("@azure/identity");
require('dotenv').config();

// Event hubs 
const eventHubsResourceName = "supng-we";
const fullyQualifiedNamespace = `${eventHubsResourceName}.servicebus.windows.net`;
const eventHubName = "pubsubevents";
const batchSize = 50;

const sampleObjectFactory = () => {
    return {
        groups: ["idf", "paca", "grandest"],
        userId: crypto.randomUUID(),
        roles: ["owner"],
        subprotocol: ""
    }
}

// Azure Identity - passwordless authentication
const credential = new DefaultAzureCredential();

async function main() {

    // Create a producer client to send messages to the event hub.
    const producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential);

    // Prepare a batch of three events.
    const batch = await producer.createBatch();
    const batchSampleObjects = new Array(batchSize).fill(sampleObjectFactory());

    Array(batchSize).fill().map(() => {
        batch.tryAdd({ body: sampleObjectFactory() })
    })
    // Send the batch to the event hub.
    await producer.sendBatch(batch);

    // Close the producer client.
    await producer.close();

    console.log(`A batch of three events have been sent to the event hub : ${ new Date().toLocaleTimeString() }`);
}

setInterval(async () =>
    await main().catch((err) => {
        console.log("Error occurred: ", err);
    }), 
    1000);