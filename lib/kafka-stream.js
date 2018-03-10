const {ConsumerStream} = require("kafka-node");
const {DataStream} = require("scramjet");

/**
 * A [scramjet.DataStream](../scramjet/blob/master/docs/data-stream.md) augmented with Kafka specific methods.
 *
 * @extends DataStream
 */
class KafkaStream extends DataStream {

    /**
     * Creates a stream from kafka.
     *
     * @param {Client|KafkaClient} client KafkaNode client to Zookeeper or direcly Kafka
     * @param {Object} [consumerOptions={}] Consumer options
     * @param {Object} streamOptions Options passed to scramjet
     */
    constructor(client, options = {}, ...streamOptions) {
        super(...streamOptions);
        this._connected = false;
        this._kafka_options = {
            client,
            payloads: [],
            options
        };
    }

    /**
     * Opens up connection to kafka and starts streaming.
     * @chainable
     */
    connect() {
        const {
            client,
            payloads,
            options
        } = this._kafka_options;

        this._consumer = new ConsumerStream(client, payloads, options);
        this._connected = true;
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
        this._kafka_options.payloads.push(...topics);
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
        this.options.payloads = this.options.payloads.filter(
            (topic) => topics.indexOf(typeof topic === "string" ? topic : topic.topic) > -1
        );

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

module.exports = {KafkaStream};
