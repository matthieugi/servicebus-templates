const { delay, ServiceBusClient, ServiceBusMessage } = require("@azure/service-bus");
require('dotenv').config();

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING

// name of the topic
const topicName = "production"
const subscriptionName = "vm1"

let nbReceivedMessages = 0;

 async function main() {
    // create a Service Bus client using the connection string to the Service Bus namespace
    const sbClient = new ServiceBusClient(connectionString);

    // createReceiver() can also be used to create a receiver for a subscription.
    const receiver = sbClient.createReceiver(topicName, subscriptionName, {
        receiveMode: "peekLock"
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

    // // Waiting long enough before closing the sender to send messages
    // await delay(20000);

    // await receiver.close(); 
    // await sbClient.close();
}

//
setInterval(() => {
    console.log(`${ new Date(Date.now()).toLocaleString() } ${nbReceivedMessages} received`);
    nbReceivedMessages = 0;
}, 60000)

// call the main function
main().catch((err) => {
    console.log("Error occurred: ", err);
    process.exit(1);
});

 