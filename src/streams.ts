/*!
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as common from '@google-cloud/common';
import {paginator, ResourceStream} from '@google-cloud/paginator';
import {promisifyAll} from '@google-cloud/promisify';
import arrify = require('arrify');
import Big from 'big.js';
import * as extend from 'extend';
import pEvent from 'p-event';
import * as fs from 'fs';
import * as is from 'is';
import * as path from 'path';
import * as streamEvents from 'stream-events';
import * as uuid from 'uuid';
import {
  BigQuery,
  Job,
  Dataset,
  Query,
  SimpleQueryRowsResponse,
  SimpleQueryRowsCallback,
  ResourceCallback,
  RequestCallback,
  PagedResponse,
  PagedCallback,
  JobRequest,
  Table,
  PagedRequest,
} from '.';
import {GoogleErrorBody, MakeAuthenticatedRequestFactoryConfig} from '@google-cloud/common/build/src/util';
import {Duplex, Writable} from 'stream';
import {JobMetadata} from './job';
import bigquery from './types';
import {EventEmitter} from 'events';
import {Publisher} from './publisher'
import {RowBatch} from './rowBatch'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const duplexify = require('duplexify');

// This is supposed to be a @google-cloud/storage `File` type. The storage npm
// module includes these types, but is current installed as a devDependency.
// Unless it's included as a production dependency, the types would not be
// included.  The storage module is fairly large, and only really needed for
// types.  We need to figure out how to include these types properly.
export interface File {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  bucket: any;
  kmsKeyName?: string;
  userProject?: string;
  name: string;
  generation?: number;
}

export type JobMetadataCallback = RequestCallback<JobMetadata>;
export type JobMetadataResponse = [JobMetadata];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type RowMetadata = any;

export type InsertRowsOptions = bigquery.ITableDataInsertAllRequest & {
  createInsertId?: boolean;
  partialRetries?: number;
  raw?: boolean;
  schema?: string | {};
};

export type InsertRowsResponse = [
  bigquery.ITableDataInsertAllResponse | bigquery.ITable
];
export type InsertRowsCallback = RequestCallback<
  bigquery.ITableDataInsertAllResponse | bigquery.ITable
>;

export type RowsResponse = PagedResponse<
  RowMetadata,
  GetRowsOptions,
  bigquery.ITableDataList | bigquery.ITable
>;
export type RowsCallback = PagedCallback<
  RowMetadata,
  GetRowsOptions,
  bigquery.ITableDataList | bigquery.ITable
>;

export interface InsertRow {
  insertId?: string;
  json?: bigquery.IJsonObject;
}

export type TableRow = bigquery.ITableRow;
export type TableRowField = bigquery.ITableCell;
export type TableRowValue = string | TableRow;

export type GetRowsOptions = PagedRequest<bigquery.tabledata.IListParams>;

export type JobLoadMetadata = JobRequest<bigquery.IJobConfigurationLoad> & {
  format?: string;
};

export type CreateExtractJobOptions = JobRequest<
  bigquery.IJobConfigurationExtract
> & {
  format?: 'CSV' | 'JSON' | 'AVRO' | 'PARQUET' | 'ORC';
  gzip?: boolean;
};

export type JobResponse = [Job, bigquery.IJob];
export type JobCallback = ResourceCallback<Job, bigquery.IJob>;

export type CreateCopyJobMetadata = CopyTableMetadata;
export type SetTableMetadataOptions = TableMetadata;
export type CopyTableMetadata = JobRequest<bigquery.IJobConfigurationTableCopy>;

export type TableMetadata = bigquery.ITable & {
  name?: string;
  schema?: string | TableField[] | TableSchema;
  partitioning?: string;
  view?: string | ViewDefinition;
};

export type ViewDefinition = bigquery.IViewDefinition;
export type FormattedMetadata = bigquery.ITable;
export type TableSchema = bigquery.ITableSchema;
export type TableField = bigquery.ITableFieldSchema;

export interface PartialInsertFailure {
  message: string;
  reason: string;
  row: RowMetadata;
}

/**
 * The file formats accepted by BigQuery.
 *
 * @type {object}
 * @private
 */
const FORMATS = {
  avro: 'AVRO',
  csv: 'CSV',
  json: 'NEWLINE_DELIMITED_JSON',
  orc: 'ORC',
  parquet: 'PARQUET',
} as {[index: string]: string};

export interface TableOptions {
  location?: string;
}

import {BATCH_LIMITS} from './publisher';

export interface BatchPublishOptions {
  maxBytes?: number;
  maxMessages?: number;
  maxMilliseconds?: number;
}

const defaultOptions = {
    // The maximum number of messages we'll batch up for publish().
    maxOutstandingMessages: 500,

    // The maximum size of the total batched up messages for publish().
    maxOutstandingBytes: 1 * 1024 * 1024,

    // The maximum time we'll wait to send batched messages, in milliseconds.
    maxDelayMillis: 10000,

}

export abstract class RowQueue extends EventEmitter {
  batchOptions: any;
  publisher: any;
  table: any;
  insertOpts: any;
  pending?: NodeJS.Timer;
  stream: any;
  constructor(table: any, dup: any, options?: any) {
    super();
    // const {insertOpts, batchOpts} = options!;
    this.table = table;
    // this.insertOpts = insertOpts;
    // this.publisher = publisher;
    // this.batchOptions = options || this.setOptions();
    this.stream = dup
    // this.batchOptions = publisher.settings.batching!;
  }
  /**
   * Adds a message to the queue.
   *
   * @abstract
   *
   * @param {object} message The message to publish.
   * @param {PublishCallback} callback The publish callback.
   */
  abstract add(data: any, callback: any): void;
  /**
   * Method to initiate publishing.
   *
   * @abstract
   */
  abstract publish(): void;
  /**
   * Accepts a batch of messages and publishes them to the API.
   *
   * @param {object[]} messages The messages to publish.
   * @param {PublishCallback[]} callbacks The corresponding callback functions.
   * @param {function} [callback] Callback to be fired when publish is done.
   */
  _publish(
    rows: any,
    // options: any,
    callbacks: any[],
    callback?: any
  ): void {
    // const {table, settings} = this.publisher;
    // const {rows, options= {}} = dup;
    // const reqOpts = {
    //   table: table.name,
    //   messages,
    // };
    // const sendStream = duplexify()
    // const req = {json: rows}
    // const buf = Buffer.from(JSON.stringify(req))
    // sendStream.push(buf)
    const uri = `http://${this.table.bigQuery.apiEndpoint}/bigquery/v2/projects/${this.table.bigQuery.projectId}/datasets/${this.table.dataset.id}/tables/${this.table.id}/insertAll`

    const reqOpts = {
      uri
    }
    // this.table._insert(rows, options,(err: any, resp: any) => {
    //   if (typeof callback === 'function') {
    //     callback(err, resp);
    //   }
    // });
 
    const json = extend(true, {}, {rows});
    this.table.request({
      method: 'POST',
      uri: '/insertAll',
      json,
      }, (err:any, resp:any) => {

      const partialFailures = (resp.insertErrors || []).map(
        (insertError: GoogleErrorBody) => {
          return {
            errors: insertError.errors!.map(error => {
              return {
                message: error.message,
                reason: error.reason,
              };
            }),
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            row: rows[(insertError as any).index],
          };
        }
      );

      if (partialFailures.length > 0) {
        throw new common.util.PartialFailureError({
          errors: partialFailures,
          response: resp,
        } as GoogleErrorBody);
      }
      callbacks.forEach((callback) => callback(err, resp));
      // if (typeof callback === 'function') {
      //   console.log('***')
      //   this.emit('response', resp)
      //   callback(err);
      // }
      console.log('***')
      this.emit('response', resp)
      callback(err);
      // console.log('***')
      // this.stream.emit('response', resp)
    })
    
    // common.util.makeWritableStream(sendStream, {
    //   makeAuthenticatedRequest: (reqOpts: object) => {
    //     this.table.request(reqOpts as any, (err:any, body:any, resp:any) => {
    //       if (err) {
    //         this.stream.destroy(err);
    //         return;
    //       }

    //       // this.metadata = body;
    //       this.stream.emit('response', resp);
    //       this.stream.emit('complete');
    //     });
    //   },
    //   request: reqOpts,
    //   // metadata: options.metadata,
    //   // request: reqOpts,
    // });
  }


}

export type rowBatch = any;
/**
 * Standard row queue used for publishing rows.
 *
 * @private
 * @extends rowQueue
 *
 * @param {Publisher} publisher The publisher.
 */
export class Queue extends RowQueue {
  batch: rowBatch;
  batchOptions: any;
  error: any;
  batches:any
  inFlight: any;
  constructor(table: any, dup: any, options?: any) {
    super(table, dup, options);
    if(typeof options === 'object') {
      this.batchOptions = options}
    else {this.setOptions();}
    this.batch = new RowBatch(this.batchOptions);
    this.inFlight = false;
  }
  /**
   * Adds a row to the queue.
   *
   * @param {Pubsubrow} row The row to publish.
   * @param {PublishCallback} callback The publish callback.
   */
  add(row: any, callback: any): void {
    row = {json: Table.encodeValue_(row)};
    // if (options.createInsertId !== false) {
      row.insertId = uuid.v4();
    // }

    // if (this.error) {
    //   callback(this.error);
    //   return;
    // }
    // row = JSON.stringify(row)
    // row = row.replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g, '"$2": '); 
    // row = JSON.parse(row)
    if (!this.batch.canFit(row)) {
      this.publish();
    }
    
    this.batch.add(row, callback!);

    if (this.batch.isFull()) {
      this.publish();
    } else if (!this.pending) {
      const {maxMilliseconds} = this.batchOptions;
      this.pending = setTimeout(() => {
        console.log("publishing")
        console.log(this.batch)
        this.publish()
      }, maxMilliseconds!);
    }
  }
  /**
   * Cancels any pending publishes and calls _publish immediately.
   */
  publish(callback?: any): void {
    const {rows, callbacks} = this.batch;
    const definedCallback = callback || (() => {});

    this.batch = new RowBatch(this.batchOptions);

    if (this.pending) {
      clearTimeout(this.pending);
      delete this.pending;
    }

    this._publish(rows, callbacks, callback
    //   (err: any, resp:any) => {
    //   // this.inFlight = false;

    //   if (err) {
    //     // this.handlePublishFailure(err);
    //     definedCallback(err, resp);
    //   // } else if (this.batches.length) {
    //   //   this.beginNextPublish();
    //   } else {
    //     this.stream.emit('response', resp)
    //     // this.stream.emit('drain');
    //     // this.stream.emit('complete')
    //     // definedCallback(null, resp);
    //   }
    // }
    );
  }
  // get currentBatch(): any {
  //   if (!this.batches.length) {
  //     this.batches.push(this.createBatch());
  //   }
  //   return this.batches[0];
  // }
  // handlePublishFailure(err: any): void {
  //   this.error = new Error(this.key, err);

  //   // reject all pending publishes
  //   while (this.batches.length) {
  //     const {callbacks} = this.batches.pop()!;
  //     callbacks.forEach(callback => callback(err));
  //   }
  // }

  setOptions(options?: any): void {
    const defaults = {
      batching: {
        maxBytes: defaultOptions.maxOutstandingBytes,
        maxMessages: defaultOptions.maxOutstandingMessages,
        maxMilliseconds: defaultOptions.maxDelayMillis,
      },
    };
    
    const opts = (typeof options === 'object')  ? options : defaults
  
    // const {
    //   batching,
    // } = extend(true, defaults, options);
  
  
    this.batchOptions = {
        maxBytes: Math.min(opts.batching.maxBytes, BATCH_LIMITS.maxBytes!),
        maxMessages: Math.min(opts.batching.maxMessages, BATCH_LIMITS.maxMessages!),
        maxMilliseconds: opts.batching.maxMilliseconds,
    };
  
   }
}