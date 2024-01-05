import {open } from 'node:fs/promises';
import { MessageChannel, Worker, isMainThread, parentPort } from 'node:worker_threads';

let globalMap = new Map();


const numWorkers = 12;

let doneCount = 0;


let workers = [];

let startTs = null;
let endTs = null;
async function runProgram() {
  startTs = process.hrtime.bigint(); 
  const file = await open(process.argv[2]);
  let stats = await file.stat();
  let workerSize = Math.floor(stats.size / numWorkers);
  console.log(`totalsize is ${stats.size}, workerSize = ${workerSize}`);
  workers = [];
  for(let i=0;i<numWorkers;i++) {
   let worker = new Worker("./worker.mjs", {workerData: {filepath : process.argv[2], idx: i, workerSize: workerSize, totalSize: stats.size}}); 
   worker.on('message', ({idx, stats}) => { mergeMap(stats); });
  }
}

function mergeMap(workerMap) {
 for(const key of workerMap.keys()) {
   if(globalMap.has(key)) {
      let localvalues = workerMap.get(key);
      let globalvalues = globalMap.get(key);
      globalvalues.total += localvalues.total;
      globalvalues.count += localvalues.count;
      if(localvalues.min < globalvalues.min) { globalvalues.min = localvalues.min;}
      if(localvalues.max > globalvalues.max) { globalvalues.max = localvalues.max;}
   } else {
      globalMap.set(key, workerMap.get(key)); 
   } 
 } 
 doneCount += 1;
 if(doneCount == numWorkers) {
   printResults(globalMap);
 }
}

function printResults(map) {
  let totalRowCount = 0;
  for(const city of map.keys()) {
    let {min, max, total, count} = map.get(city);
    totalRowCount += count;
  }
  endTs = process.hrtime.bigint();
  for(const city of map.keys()) {
    let {min, max, total, count} = map.get(city);
    console.log(`${city}: min: ${min}, max: ${max}, avg: ${total/count}`);
  }

  console.log(`totalrows seen ${totalRowCount}`);
  console.log(`total time is ${endTs-startTs} nanoseconds`); 
  process.exit(0);
}

runProgram();

