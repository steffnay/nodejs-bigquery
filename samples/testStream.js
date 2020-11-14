const fs = require('fs');
const {Readable, Duplex, Transform} = require('stream');
const util = require('util');
const through2 = require('through2');
// var Transform = require('stream').Transform;
const {
  Table,
  BigQuery,
  encodeRows,
  Publisher,
} = require('@google-cloud/bigquery');
// const { file } = require('tmp');
const streamEvents = require('stream-events');
const bigquery = new BigQuery();

const datasetId = 'testing';
const tableId = 'insert_table_yay';

const rows = [
  {name: 'Tom', age: 30},
  {name: 'Jane', age: 32},
];
const buffer = Buffer.from(JSON.stringify(rows));
const table = bigquery.dataset(datasetId).table(tableId);

// const pub = new Publisher(table);

const file = require('./json.json');
const {nextTick} = require('process');
const {json} = require('is');
// var s2 = require('./sensor2.js');

// // var minLength = s1stream.length;
// //console.log(s1);
// function s1stream() {
//   Duplex.call(this)
//   streamEvents.call(this)
// }

// util.inherits(s1stream, Duplex)
// var s1stream = new Duplex({
//    objectMode: true,
//   //  write: () => {
//   //    next()}

//  })

//  s1stream.prototype._read = (chunk)=>{
//   console.log('_read called as usual')
//   this.push(chunk)
//   this.push(null)
// }

// // s1stream._write = () =
//  s1stream.on('reading', () => {
//   // const string = file.toString()
//   // console.log(file)
//   // this.push(file)
//   //   cb()
//   console.log('s1 reading')
// })

// s1stream.on('writing', () => {
//   // const string = file.toString()
//   // console.log(file)
//   // this.push(file)
//   //   cb()
//   console.log('s1 writing')
// })

//  const s2stream = new Readable({
//     objectMode: true,
//     read() {}
//   })

// if (s2.length < minLength){
//   minLength = s2.length;
// }

// var n1 = 0;
// setInterval(function() {
//     if (n1 < file.length) {
//         s1stream.push(file[n1]);
//         n1++;
//     } else if (n1 == file.length) {
//         s1stream.push(null);
//     }
// }, 1000);

// var n2 = 0;
// setInterval(function() {
//     if (n2++ < minLength) {
//         s2stream.push(s2[n2]);
//     } else if (n2++ === minLength) {
//         s2stream.push(null);
//     }
// }, 1000);

const jsonStream = through2.obj(function(file, encoding, cb) {
  // const string = file.toString()
  this.push(file);
  cb();
});

const myStreamy = new Duplex({
  read(size) {
    console.log('read: ', size);
  },
  objectMode: true,
});

// const myTransform = new myStreamy({objectMode: true});
// myTransform.setEncoding('ascii');
// myTransform.on('data', (chunk) => {
//   console.log(chunk)
// })

let n1 = 0;

const funct = () => {
  if (n1 < file.length) {
    myStreamy.push(file[n1]);
    n1++;
  } else {
    clearInterval(id);
    // myStreamy.push(null);
  }
};
const id = setInterval(funct, 10);
// myStreamy.push(file[1]);
// myStreamy.push(file[2]);
// myStreamy.push(file[3]);

const code = 1;
const inoutStream = new Duplex({
  write(chunk, encoding, callback) {
    console.log(chunk.toString());
    console.log('stef ');
    callback();
  },
  objectMode: true,
});

// inoutStream.push({robots: 'steffany!', age: 30})
// myTransform.write(1);
// // Prints: 01
// myTransform.write(10);
// // Prints: 0a
// myTransform.write(100);
// // Prints: 64

// var writable = fs.createWriteStream("loadedreports/bot"+x[6]);
// request.on('row', function(result) {
//    writable.write(result);
// });

// const fileStream = fs.createReadStream('./samples/json.json');
const writer = table.createInsertStream_();
myStreamy.pipe(writer);
// myTransform.write(1);
// // Prints: 01
// myTransform.write(10);
// // Prints: 0a
// myTransform.write(100);
// Prints: 64
