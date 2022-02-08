const { Worker, MessageChannel, MessagePort, parentPort } = require('worker_threads')
require('dotenv').config();
/**
* Use a worker via Worker Threads module to make intensive CPU task
* @param filepath string relative path to the file containing intensive CPU task code
* @return {Promise(mixed)} a promise that contains result from intensive CPU task
*/

let nbMessages = 0;

function _useWorker (filepath) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(filepath)
    worker.on('online', () => { console.log(`thread ${worker.threadId} as started`) });
    worker.on('message', messageFromWorker => {
      nbMessages += Number.parseInt(messageFromWorker);
    });
    worker.on('error', reject)
    worker.on('exit', code => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`))
      }
    })
  })
}

async function main () {
    let workerThreads = [];
    for(let i=0; i < 8; i++){
        workerThreads.push(_useWorker('./send.js'));
    }

    Promise.allSettled(workerThreads);
}

main()
setInterval(() => {
    console.log(`${nbMessages} envoyés`);
    nbMessages = 0;
}, 1000);