const { delay, ServiceBusClient, ServiceBusMessage } = require("@azure/service-bus");
require('dotenv').config();

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING;

// name of the queue
const queueName = process.env.SERVICEBUS_QUEUENAME;

let nbReceivedMessages = 0;

 async function main() {
    // create a Service Bus client using the connection string to the Service Bus namespace
    const sbClient = new ServiceBusClient(connectionString);

    let receivers = [];

    for(let nbQueues=0; nbQueues < 8; nbQueues++){
        receivers.push(await sbClient.acceptSession(process.env.SERVICEBUS_QUEUENAME, `ligne${nbQueues}`));
    }

    // function to handle messages
    const myMessageHandler = async (messageReceived) => {
        nbReceivedMessages++;
    };

    // function to handle any errors
    const myErrorHandler = async (error) => {
        console.log(error);
    };

    receivers.forEach(receiver => {
        receiver.subscribe({
            processMessage: myMessageHandler,
            processError: myErrorHandler
        }, {
            maxConcurrentCalls: 1000,
        });
    });
}

//
setInterval(() => {
    console.log(`${nbReceivedMessages} received`);
    nbReceivedMessages = 0;
}, 1000)

// call the main function
main().catch((err) => {
    console.log("Error occurred: ", err);
    process.exit(1);
});

 