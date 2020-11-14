const {BigQuery} = require('@google-cloud/bigquery');

const bigquery = new BigQuery();

(async () => {
  const query = 'SELECT * from UNNEST(@points)';
  const geo = bigquery.geography('POINT(1 2)');
  const params = {
    points: [
      {
        lnglat: bigquery.geography('POINT(1 2)'),
        stamp: bigquery.timestamp('2020-01-01'),
      },
    ],
  };
  const resp = await bigquery.getCredentials();
  const email = resp.client_email;
  const binding = {
    role: 'roles/bigquery.dataViewer',
    members: ['serviceAccount:' + email],
  };
  const binding2 = {
    role: 'roles/bigquery.dataOwner',
    members: ['serviceAccount:' + email],
  };
  const policy = {
    bindings: [binding, binding2],
    // version: 1
  };
  const perm = 'bigquery.tables.get';
  const datasetId = 'loc_test';
  const tableId = 'people';
  const table = bigquery.dataset(datasetId).table(tableId);
  // table.setIamPolicy(policy, (err, resp)=>{console.log(resp)})
  table.getIamPolicy({requestedPolicyVersion: null}, (err, resp) => {
    console.log(resp);
  });
  // const [pol] = await table.testIamPermissions([perm]);
  // console.log(pol);
  // console.log(pol)
  // await table.setIamPolicy()

  // const [job] = await bigquery.createQueryJob({query, params});

  // // Wait for the query to finish
  // const [rows] = await job.getQueryResults();

  // Print the results
  // console.log('Rows:');
  // console.log(rows);
  // bigquery.query(
  //   {
  //     query: 'SELECT ? cool_place',
  //     params: [bigquery.geography('POINT(1 2)')],
  //   },
  //   (err, rows) => {
  //     console.log('Rows:');
  //     console.log(rows);
  //   }
  // );
  // BigQuery.valueToQueryParameter_.isCustomType({type: 'GEOGRAPHY'});
})();
