const { Worker, MessageChannel, MessagePort, parentPort } = require('worker_threads');
const { ServiceBusAdministrationClient, delay } = require("@azure/service-bus");
require('dotenv').config();

/**
* Use a worker via Worker Threads module to make intensive CPU task
* @param filepath string relative path to the file containing intensive CPU task code
* @return {Promise(mixed)} a promise that contains result from intensive CPU task
*/

let nbMessages = 0;

// name of the queue
const topicName = process.env.TOPICNAME;

// connection string to your Service Bus namespace
const connectionString = process.env.SERVICEBUS_CONNECTIONSTRING;

function _useWorker(filepath) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(filepath);
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
      process.exit();
    })
  })
}

async function main() {
  // create an Admin Client to manage topics
  const sbAdminClient = new ServiceBusAdministrationClient(connectionString);

  // create a topic with options
  await sbAdminClient.createTopic(topicName, {
    defaultMessageTimeToLive: 'PT30M',
  });

  
  await delay(5000);


  let workerThreads = [];
  for (let i = 0; i < 2; i++) {
    workerThreads.push(_useWorker('./send.js'));
  }

  Promise.allSettled(workerThreads);
}

main()
setInterval(() => {
  console.log(`${new Date(Date.now()).toLocaleString()} ${nbMessages} envoyés`);
  nbMessages = 0;
}, 60000);