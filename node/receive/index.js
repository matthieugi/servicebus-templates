const { delay } = require('@azure/service-bus');
const { syncBuiltinESMExports } = require('module');
const { Worker, MessageChannel, MessagePort, parentPort } = require('worker_threads');
require('dotenv').config();

/**
* Use a worker via Worker Threads module to make intensive CPU task
* @param filepath string relative path to the file containing intensive CPU task code
* @return {Promise(mixed)} a promise that contains result from intensive CPU task
*/

let nbMessages = 0;
const topicName = process.env.TOPICNAME;

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
      process.exit();
    })
  })
}

async function main() {
  await _useWorker('./receive.js', topicName);
}

main()
setInterval(() => {
  console.debug(`${new Date(Date.now()).toLocaleString()} ${nbMessages} recus`);
  nbMessages = 0;
}, 60000);