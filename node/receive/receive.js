const { delay, ServiceBusClient, ServiceBusMessage, isServiceBusError, ServiceBusAdministrationClient  } = require("@azure/service-bus");
const { parentPort, threadId } = require('worker_threads');

require('dotenv').config();
const os = require('os');

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING

// name of the topic
const topicName = process.env.TOPICNAME;
let nbReceivedMessages = 0;

console.debug(`topicName is : ${ topicName }`);
let receivers = [];
const subscriptionNames = ['low', 'high', 'direct'];
const sbAdminClient = new ServiceBusAdministrationClient(connectionString);

// create a Service Bus client using the connection string to the Service Bus namespace
const sbClient = new ServiceBusClient(connectionString, {});


const initialize = async () => {
    subscriptionNames.forEach(async subscriptionName => {
        await sbAdminClient.createSubscription(topicName, subscriptionName, {
            lockDuration: "PT30S",
            autoDeleteOnIdle: "PT5M",
            maxDeliveryCount: 1
        })
    });
    return subscriptionNames;
}

parentPort.on('message', async (message) => {
    if(message.exit){
        subscriptionNames.forEach(async subscriptionName => {
            let receiversPromises = [];

            receivers.forEach(receiver => receiversPromises.push(receiver.close()));
            
            await sbAdminClient.deleteSubscription(topicName, subscriptionName),
            await Promise.allSettled([
                sbClient.close(),
                ...receiversPromises
            ]); 
        });
        console.log('1');
        await delay(5000);
        process.exit();
    }
});

const receive = async (subscriptionNames) => {
    subscriptionNames.forEach(async subscriptionName => {
        // createReceiver() can also be used to create a receiver for a subscription.
        receivers.push(sbClient.createReceiver(topicName, subscriptionName, {
            receiveMode: "peekLock",
    
        }));        
    })

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
        }
    };

    receivers.forEach(receiver => {
        // subscribe and specify the message and error handlers
        receiver.subscribe({
            processMessage: myMessageHandler,
            processError: myErrorHandler
        }, {
            maxConcurrentCalls: 20,
        });
    })
}

async function main() {
    const subscriptionNames = await initialize();
    await receive(subscriptionNames);
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

 