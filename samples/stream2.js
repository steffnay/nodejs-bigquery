const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const fs = require('fs');

const datasetId = 'loc_test';
const tableId = 'people';

const table = bigquery.dataset(datasetId).table(tableId);

const fileStream = table.createWriteStream();
fs.createReadStream(
  '/Users/steffanyb/google/testing/bigquery-test-project/data/stringRows.csv'
)
  .pipe(fileStream)
  .on('error', err => {})
  .on('finish', () => {
    console.log('Finished!');
  });

const request = require('request');

const csvUrl = 'http://goo.gl/kSE7z6';

const options = {
  insertOptions: {
    ignoreUnknownValues: true,
    createInsertId: false,
  },
  batchingOptions: {
    maxMessages: 300,
  },
};

request
  .get(csvUrl)
  .pipe(table.createInsertStream(options))
  .on('response', response => {
    console.log(response);
  })
  .on('error', err => {
    console.log(err);
  });
