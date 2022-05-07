const { Worker, MessageChannel, MessagePort, parentPort } = require('worker_threads');
const { ServiceBusAdministrationClient, delay } = require("@azure/service-bus");
require('dotenv').config();

/**
* Use a worker via Worker Threads module to make intensive CPU task
* @param filepath string relative path to the file containing intensive CPU task code
* @return {Promise(mixed)} a promise that contains result from intensive CPU task
*/

let nbMessages = 0;

//nb ob topics in the namespace
const nbTopics = Number.parseInt(process.env.NBTOPICS)

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING;

//Service bus admin client
const sbAdminClient = new ServiceBusAdministrationClient(connectionString)

function _useWorker(filepath, topicName) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(filepath, {
      env: {
        TOPICNAME: topicName
      }
    });
    worker.on('online', () => { console.log(`thread ${worker.threadId} as started`) });
    worker.on('message', messageFromWorker => {
      nbMessages += Number.parseInt(messageFromWorker);
    });
    worker.on('error', reject)
    worker.on('exit', code => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`))
      }
    });
    process.on('SIGINT', async () => {
      worker.postMessage({ exit: true });

      await delay(5000);

      await sbAdminClient.deleteTopic(topicName);
      process.exit();
    })
  })
}

async function main() {
  // create an Admin Client to manage topics;
  let workerThreads = [];

  for (let i = 0; i < nbTopics; i++) {
    //topic Name
    const topicName = `dauphine-publi-${i}`;

    // create a topic with options
    await sbAdminClient.createTopic(topicName, {
      defaultMessageTimeToLive: 'PT30M',
    });

    await delay(1000);

    for (let i = 0; i < 2; i++) {
      workerThreads.push(_useWorker('./send.js', topicName));
    }
  }

  Promise.allSettled(workerThreads);
}

main()
setInterval(() => {
  console.log(`${new Date(Date.now()).toLocaleString()} ${nbMessages} envoyés`);
  nbMessages = 0;
}, 60000);