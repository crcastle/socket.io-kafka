/**
 * Kafka adapter for socket.io
 *
 * @example <caption>Example usage</caption>
 * var io = require('socket.io')(3000);
 * var kafka = require('socket.io-kafka');
 * io.adapter(kafka(process.env.KAFKA_URL, {
 *      topic: process.env.KAFKA_PREFIX+process.env.KAFKA_TOPIC,
 *      createTopics: false
 * }));
 *
 * @module socket.io-kafka
 * @see {@link https://www.npmjs.com/package/kafka-node|kafka-node}
 *
 * @author Guilherme Hermeto
 * @licence {@link http://opensource.org/licenses/MIT|MIT}
 */

/*jslint node: false */

'use strict';

var kafka = require('no-kafka'),
    Adapter = require('socket.io-adapter'),
    debug = require('debug')('socket.io-kafka'),
    uid2 = require('uid2');

/**
 * Generator for the kafka Adapater
 *
 * @param {string} optional, kafka connection string
 * @param {object} adapter options
 * @return {Kafka} adapter
 * @api public
 */
function adapter(uri, options) {
    var opts = options || {},
        prefix = opts.key || 'socket.io',
        uid = uid2(6),
        client;

    // handle options only
    if ('object' === typeof uri) {
        opts = uri;
        uri = opts.uri || opts.host ? opts.host + ':' + opts.port : null;
        if (!uri) { throw new URIError('URI or host/port are required.'); }
    }

    // create producer and consumer if they weren't provided
    if (!opts.producer || !opts.consumer) {
        if (!opts.producer) {
            debug('creating new kafka producer');
            opts.producer = new kafka.Producer({ connectionString: uri });
        }
        if (!opts.consumer) {
            debug('creating new kafka consumer');
            opts.consumer = new kafka.SimpleConsumer({ connectionString: uri });
        }
    }
    /**
     * Kafka Adapter constructor.
     *
     * @constructor
     * @param {object} channel namespace
     * @api public
     */
    function Kafka(nsp) {
        var self = this,
            create = opts.createTopics;

        Adapter.call(this, nsp);

        this.uid = uid;
        this.options = opts;
        this.prefix = prefix;
        this.consumer = opts.consumer;
        this.producer = opts.producer;
        this.topic = opts.topic;
        opts.createTopics = (create === undefined) ? true : create;

        // TODO: replace this junk with async/await
        this.producer.init()
        .then(() => debug('Producer initialized.'))
        .catch(e => debug('Error initializing producer.', e));
        
        this.consumer.init()
        .then(() => {
            debug('Consumer initialized')
            self.consumer.subscribe(self.topic, (messageSet, topic, partition) => {
                messageSet.forEach(m => {
                    self.onMessage(m.message);
                });
            });  
        })
        .catch(e => debug('Error initializing consumer', e));
    }

    // inherit from Adapter
    Kafka.prototype = Object.create(Adapter.prototype);
    Kafka.prototype.constructor = Kafka;

    /**
     * Emits the error.
     *
     * @param {object|string} error
     * @api private
     */
    Kafka.prototype.onError = function (err) {
        var self = this,
            arr = [].concat.apply([], arguments);

        if (err) {
            debug('emitting error', err);
            arr.forEach(function (error) { self.emit('error', error); });
        }
    };

    /**
     * Process a message received by a consumer. Ignores messages which come
     * from the same process.
     *
     * @param {object} kafka message
     * @api private
     */
    Kafka.prototype.onMessage = function (kafkaMessage) {
        var message, packet;

        try {
            message = JSON.parse(kafkaMessage.value);
            if (uid === message[0]) { return debug('ignore same uid'); }
            packet = message[1];

            if (packet && packet.nsp === undefined) {
                packet.nsp = '/';
            }

            if (!packet || packet.nsp !== this.nsp.name) {
                return debug('ignore different namespace');
            }

            this.broadcast(packet, message[2], true);
        } catch (err) {
            // failed to parse JSON?
            this.onError(err);
        }
    };

    /**
     * Uses the producer to send a message to kafka. Uses snappy compression.
     *
     * @param {string} topic to publish on
     * @param {object} packet to emit
     * @param {object} options
     * @api private
     */
    Kafka.prototype.publish = function (packet, opts) {
        var self = this,
            msg = JSON.stringify([self.uid, packet, opts]);

        // TODO: add back snappy compression
        this.producer.send({
          topic: self.topic,
          partition: 0,
          message: { value: msg }
        })
        .then(result => debug('result:', result))
        .catch(err => self.onError(err));
    };

    /**
     * Broadcasts a packet.
     *
     * If remote is true, it will broadcast the packet. Else, it will also
     * produces a new message in one of the kafka topics (channel or rooms).
     *
     * @param {object} packet to emit
     * @param {object} options
     * @param {Boolean} whether the packet came from another node
     * @api public
     */
    Kafka.prototype.broadcast = function (packet, opts, remote) {
        var self = this,
            channel;

        debug('broadcasting packet', packet, opts);
        Adapter.prototype.broadcast.call(this, packet, opts);

        if (!remote) {
            self.publish(packet, opts);
        }
    };

    return Kafka;
}

module.exports = adapter;
