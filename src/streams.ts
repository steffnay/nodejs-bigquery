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
  PagedRequest,
} from '.';
import {GoogleErrorBody} from '@google-cloud/common/build/src/util';
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

export abstract class RowQueue extends EventEmitter {
  batchOptions: any;
  publisher: any;
  pending?: NodeJS.Timer;
  constructor(publisher: any) {
    super();
    this.publisher = publisher;
    this.batchOptions = publisher.settings.batching!;
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
    dup: any,
    callbacks: any[],
    callback?: any
  ): void {
    const {table, settings} = this.publisher;
    // const {rows, options= {}} = dup;
    // const reqOpts = {
    //   table: table.name,
    //   messages,
    // };


    common.util.makeWritableStream(dup, {
      makeAuthenticatedRequest: table.bigQuery.makeAuthenticatedRequest,
      // metadata: options.metadata
      request: {
        method: 'POST',
        uri: '/insertAll'}
      }, // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (err:any, body:any, resp:any) => {
      if (err) {
        dup.destroy(err);
        return;
      }
      dup.emit('response', {body, resp});
      dup.emit('complete');
    });
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
  constructor(publisher: Publisher) {
    super(publisher);
    this.batch = new RowBatch(this.batchOptions);
  }
  /**
   * Adds a row to the queue.
   *
   * @param {Pubsubrow} row The row to publish.
   * @param {PublishCallback} callback The publish callback.
   */
  add(row: any, callback: any): void {
    if (!this.batch.canFit(row)) {
      this.publish();
    }

    this.batch.add(row, callback);

    if (this.batch.isFull()) {
      this.publish();
    } else if (!this.pending) {
      const {maxMilliseconds} = this.batchOptions;
      this.pending = setTimeout(() => this.publish(), maxMilliseconds!);
    }
  }
  /**
   * Cancels any pending publishes and calls _publish immediately.
   */
  publish(callback?: any): void {
    const {messages, callbacks} = this.batch;

    this.batch = new RowBatch(this.batchOptions);

    if (this.pending) {
      clearTimeout(this.pending);
      delete this.pending;
    }

    this._publish(messages, callbacks, callback);
  }
}