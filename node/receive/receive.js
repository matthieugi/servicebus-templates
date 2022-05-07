const { delay, ServiceBusClient, ServiceBusMessage, ServiceBusAdministrationClient  } = require("@azure/service-bus");
const { parentPort, threadId } = require('worker_threads');

require('dotenv').config();
const os = require('os');

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING

// name of the topic
const topicName = 'commerce';
console.log(`topicName is : ${ topicName }`);
const subscriptionName = `${ os.hostname() }-${ threadId }`;
const sbAdminClient = new ServiceBusAdministrationClient(connectionString);

let nbReceivedMessages = 0;

const initialize = async () => {
    await sbAdminClient.createSubscription(topicName, subscriptionName, {
        lockDuration: "PT30S",
        defaultMessageTimeToLive: "PT5M",
        autoDeleteOnIdle: "PT10M",
        maxDeliveryCount: 1
    })
    return subscriptionName;
}

parentPort.on('message', async (message) => {
    if(message.exit){
        await sbAdminClient.deleteSubscription(topicName, subscriptionName);
        process.exit();
    }
});

const receive = async (subscriptionName) => {
    // create a Service Bus client using the connection string to the Service Bus namespace
    const sbClient = new ServiceBusClient(connectionString, {
        
    });

    // createReceiver() can also be used to create a receiver for a subscription.
    const receiver = sbClient.createReceiver(topicName, subscriptionName, {
        receiveMode: "receiveAndDelete"
    });

    // function to handle messages
    const myMessageHandler = async (messageReceived) => {
        nbReceivedMessages++;
    };

    // function to handle any errors
    const myErrorHandler = async (error) => {
        console.log(error);
    };

    // subscribe and specify the message and error handlers
    receiver.subscribe({
        processMessage: myMessageHandler,
        processError: myErrorHandler
    }, {
        maxConcurrentCalls: 1000,
    });
}

async function main() {
    const subscriptionName = await initialize();
    await receive(subscriptionName);
}

//
setInterval(() => {
    parentPort.postMessage(nbReceivedMessages);
    nbReceivedMessages = 0;
}, 1000)

// call the main function
main().catch((err) => {
    console.log("Error occurred: ", err);
    process.exit(1);
});

 