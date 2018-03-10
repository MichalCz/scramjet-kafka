const {ConsumerStream, Client, KafkaClient} = require("kafka-node");
const {StringStream} = require("scramjet");

/**
 * A [scramjet.DataStream](../scramjet/blob/master/docs/data-stream.md) augmented with Kafka specific methods.
 *
 * @extends DataStream
 */
class KafkaStream extends StringStream {

    /**
     * Creates a stream from kafka.
     *
     * @param {Object} [consumerOptions={}] Consumer options
     * @param {Object} streamOptions Options passed to scramjet
     */
    constructor(options) {
        super(Object.assign({
            payloads: [],
            kafkaOptions: {},
            zkOptions: {},
            consumerOptions: {}
        }, options));
        this._connected = false;
        this._client = null;
        this._consumer = null;
    }

    /**
     * Opens up connection to kafka and starts streaming.
     * @chainable
     */
    connect() {
        this._client = getClient(this._options);

        this._consumer = new ConsumerStream(this._client, this._options.payloads, this._options.kafkaOptions);
        this._connected = true;
        this.once("end", () => this._client.close());
        return this._consumer.pipe(this);
    }

    get _raiseCb() {
        return err => err && this.raise(err);
    }

    /**
     * Add topics to the stream
     * @param {Array.<String|Topic>} topics list of topics to listen on
     * @param {Array} args additional arguments to `ConsumerStream::addTopics`
     * @chainable
     */
    addTopics(topics, ...args) {
        this._options.payloads.push(...topics);
        if (this._connected)
            this._consumer.addTopics(topics, this._raiseCb, ...args);
        return this;
    }

    /**
     * Removes topics from the stream
     *
     * @param  {String[]} topics list of topics to remove
     * @chainable
     */
    removeTopics(topics) {
        this.setOptions({payloads: this._options.payloads.filter(
            (topic) => topics.indexOf(typeof topic === "string" ? topic : topic.topic) > -1
        )});

        if (this._connected)
            this._consumer.removeTopics(topics, this._raiseCb);

        return this;
    }

    /**
     * Commits at the current position
     *
     * @chainable
     */
    commit() {
        this._consumer.commit(this._raiseCb);
        return this;
    }

    /**
     * Sets read offset at current position
     *
     * @param {String} topic topic name in kafka
     * @param {Number} partition where to start reading
     * @param {Number} offset kafka partition number
     */
    setOffset(topic, partition, offset) {
        this._consumer.setOffset(topic, partition, offset);
        return this;
    }

}

function getClient(options) {
    if (options.zkOptions)
        return new Client(options.zkOptions.connectionString, options.zkOptions.clientId, options.zkOptions.zkOptions);
    else
        return new KafkaClient(options.kafkaOptions);
}

module.exports = {
    KafkaStream,
    getClient
};
