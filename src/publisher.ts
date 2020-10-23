/*!
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import {promisify, promisifyAll} from '@google-cloud/promisify';
import * as extend from 'extend';
import {defaultOptions} from './default-options';
import {Queue } from './streams'

/**
 * @typedef PublishOptions
 * @property {BatchPublishOptions} [batching] The maximum number of bytes to
 *     buffer before sending a payload.
 * @property {object} [gaxOpts] Request configuration options, outlined
 *     {@link https://googleapis.github.io/gax-nodejs/interfaces/CallOptions.html|here.}
 * @property {boolean} [messageOrdering] If true, messages published with the
 * same order key in Message will be delivered to the subscribers in the order in which they
 *  are received by the Pub/Sub system. Otherwise, they may be delivered in
 * any order.
 */

export const BATCH_LIMITS: any = {
  maxBytes: Math.pow(1024, 2) * 9,
  maxMessages: 1000,
};

/**
 * A Publisher object allows you to publish messages to a specific topic.
 *
 * @private
 * @class
 *
 * @see [Topics: publish API Documentation]{@link https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish}
 *
 * @param {Topic} topic The topic associated with this publisher.
 * @param {PublishOptions} [options] Configuration object.
 */
export class Publisher {
  table: any;
  settings!: any;
  queue: Queue;
  // tracing: OpenTelemetryTracer | undefined;
  constructor(table: any, options?: any) {
    this.setOptions(options);
    this.table = table;
    this.queue = new Queue(this);
    // this.tracing =
    //   this.settings && this.settings.enableOpenTelemetryTracing
    //     ? new OpenTelemetryTracer()
    //     : undefined;
  }

  flush(): Promise<void>;
  flush(callback: any): void;
  /**
   * Immediately sends all remaining queued data. This is mostly useful
   * if you are planning to call close() on the PubSub object that holds
   * the server connections.
   *
   * @private
   *
   * @param {EmptyCallback} [callback] Callback function.
   * @returns {Promise<EmptyResponse>}
   */
  flush(callback?: any): Promise<void> | void {
    const definedCallback = callback ? callback : () => {};

    const publishes = [promisify(this.queue.publish)()];

    const allPublishes = Promise.all(publishes);

    allPublishes
      .then(() => {
        definedCallback(null);
      })
      .catch(definedCallback);
  }

  /**
   * Publish the provided message.
   *
   * @deprecated use {@link Publisher#publishMessage} instead.
   *
   * @private
   * @see Publisher#publishMessage
   *
   * @param {buffer} data The message data. This must come in the form of a
   *     Buffer object.
   * @param {object.<string, string>} [attributes] Attributes for this message.
   * @param {PublishCallback} [callback] Callback function.
   * @returns {Promise<PublishResponse>}
   */
  publish(
    rows: Buffer,
    optionsOrCb?: any,
    callback?: any
  ): Promise<string> | void {
    const options = typeof optionsOrCb === 'object' ? optionsOrCb : {};
    callback = typeof optionsOrCb === 'function' ? optionsOrCb : callback;
    return this.publishMessage({rows, options}, callback!);
  }
  /**
   * Publish the provided message.
   *
   * @private
   *
   * @throws {TypeError} If data is not a Buffer object.
   * @throws {TypeError} If any value in `attributes` object is not a string.
   *
   * @param {PubsubMessage} [message] Options for this message.
   * @param {PublishCallback} [callback] Callback function.
   */
  publishMessage(data: any, callback: any): void {
    const {rows, options = {}} = data;

    if (!(data instanceof Buffer)) {
      throw new TypeError('Data must be in the form of a Buffer.');
    }

//     for (const key of Object.keys(attributes!)) {
//       const value = attributes![key];
//       if (typeof value !== 'string') {
//         throw new TypeError(`All attributes must be in the form of a string.
// \nInvalid value of type "${typeof value}" provided for "${key}".`);
//       }
//     }

    // const span: Span | undefined = this.constructSpan(message);

      this.queue.add(data, callback);
      return;
  }
  /**
   * Indicates to the publisher that it is safe to continue publishing for the
   * supplied ordering key.
   *
   * @private
   *
   * @param {string} key The ordering key to continue publishing for.
   */
  // resumePublishing(key: string) {
  //   const queue = this.orderedQueues.get(key);

  //   if (queue) {
  //     queue.resumePublishing();
  //   }
  // }
  /**
   * Sets the Publisher options.
   *
   * @private
   *
   * @param {PublishOptions} options The publisher options.
   */
  setOptions(options = {} as any): void {
    const defaults = {
      batching: {
        maxBytes: defaultOptions.publish.maxOutstandingBytes,
        maxMessages: defaultOptions.publish.maxOutstandingMessages,
        maxMilliseconds: defaultOptions.publish.maxDelayMillis,
      },
    };

    const {
      batching,
    } = extend(true, defaults, options);


    this.settings = {
      batching: {
        maxBytes: Math.min(batching.maxBytes, BATCH_LIMITS.maxBytes!),
        maxMessages: Math.min(batching.maxMessages, BATCH_LIMITS.maxMessages!),
        maxMilliseconds: batching.maxMilliseconds,
      }
    };
  }
}

promisifyAll(Publisher, {
  singular: true,
  exclude: ['publish', 'setOptions'],
});
