const { delay, ServiceBusClient, ServiceBusMessage, isServiceBusError, ServiceBusAdministrationClient  } = require("@azure/service-bus");
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
        receiveMode: "receiveAndDelete",

    });

    // function to handle messages
    const myMessageHandler = async (messageReceived) => {
        nbReceivedMessages++;
    };

    // function to handle any errors
    const myErrorHandler = async (args) => {
        if (isServiceBusError(args.error)) {
            switch (args.error.code) {
              case "MessagingEntityDisabled":
              case "MessagingEntityNotFound":
              case "UnauthorizedAccess":
                // It's possible you have a temporary infrastructure change (for instance, the entity being
                // temporarily disabled). The handler will continue to retry if `close()` is not called on the subscription - it is completely up to you
                // what is considered fatal for your program.
                console.log(
                  `An unrecoverable error occurred. Stopping processing. ${args.error.code}`,
                  args.error
                );
                await subscription.close();
                break;
              case "MessageLockLost":
                console.log(`Message lock lost for message`, args.error);
                break;
              case "ServiceBusy":
                // choosing an arbitrary amount of time to wait.
                console.log(`delaying reception...`);
                await delay(5000);
                break;
            }
    };

    // subscribe and specify the message and error handlers
    receiver.subscribe({
        processMessage: myMessageHandler,
        processError: myErrorHandler
    }, {
        maxConcurrentCalls: 20        
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

 