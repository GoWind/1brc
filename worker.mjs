import {  parentPort, workerData } from 'node:worker_threads';
import { open } from 'node:fs/promises';
import readline from 'node:readline/promises';
let MAX_BUFFER_SIZE = 16384;

let statsmap = new Map();
let processedCount = 0;
let readAhead = 100;
let {filepath, idx, workerSize, totalSize } = workerData;
let startOffset = idx * workerSize;
let endOffset = (idx + 1) * workerSize -1;
let bytesProcessed = 0;
let handle =  await open(filepath, "r");
let stats =  await handle.stat();

let globalBuffer = "";
// console.log(`idx: ${idx}, start: ${startOffset}, end: ${endOffset}`);

if(startOffset > 0) {
 let prevChar = await readAtPos(handle, startOffset-1);
 if(prevChar == "\n") {
 } else {
    let nextPos = await findNewLineAfter(handle, startOffset, totalSize);
    startOffset = nextPos + 1;
 }
}



if(endOffset < totalSize) {
  let curChar = await readAtPos(handle, endOffset);
  if(curChar == "\n" || curChar == "\x00") {
  } else {
    let nextNewLine = await findNewLineAfter(handle, endOffset, totalSize);
    endOffset = nextNewLine;
  }
}

console.log(`${idx}: updated start and endOffset to ${startOffset} to ${endOffset} totaltoRead is ${endOffset-startOffset+1}`);

const readStream = await handle.createReadStream({start: startOffset, end: endOffset});

for await (const chunk of readStream) {
    let res = await handler(chunk);
    if(res == false) { 
      break;
    }
}

async function handler(chunk) {
  let updatedChunk = globalBuffer.concat(chunk);  
  let rows = updatedChunk.split("\n");
  if(rows.length) {
     if(rows[rows.length-1] == "") {
        globalBuffer = "";
        processRows(rows.slice(0, -1));
     } else {
       globalBuffer = rows[rows.length-1];
       processRows(rows.slice(0, -1));
     } 
  }
}
wrapUp();

function processRows(rows) {
  for(const row of rows) {
    processRow(row);
  }
}

function processRow(row) {
  if(row == "" || row == "\n") { console.log(`${idx} got blank row ${row}`); 
      return;
    }
    processedCount += 1;
    let [city, temp] = row.split(';');
    let tempf = parseFloat(temp);
    if(statsmap.has(city)) {
      let {min, max, total, count} = statsmap.get(city);
      if(tempf <  min) { min = temp; }  
      if(tempf > max) { max = temp; }
      statsmap.set(city, {min, max, total: total+tempf, count: count+1});
    } else {
      statsmap.set(city, {min: tempf, max: tempf, total: tempf, count: 1});
    }
}

function wrapUp() {
 console.log(`${idx} processed ${processedCount}`);
 handle.close();
 parentPort.postMessage({idx: idx, stats: statsmap});
 process.exit(0);
}

async function findNewLineAfter(handle, pos, totalSize) {
  let x = pos;
  let b = Buffer.alloc(1); 
  while(true) {
    let results = await handle.read(b, 0, b.length, x);
    if(results.bytesRead == 0) { return -1; }
    if(results.buffer.toString() == "\n") { return x;}
    x += 1;
  } 
}

async function readAtPos(handle, pos) {
  let b = Buffer.alloc(1);
  let results = await handle.read(b, 0, 1, pos);
  return results.buffer.toString();
}
