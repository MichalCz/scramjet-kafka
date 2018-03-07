const {Client, ConsumerStream, ProducerStream} = require("kafka-node");
const {DataStream} = require("scramjet");

const getConnection = (conn) => {
    if (conn instanceof Client) {
        return conn;
    } else {
        return new Client(conn);
    }
};

/**
 * A [scramjet.DataStream](../scramjet/blob/master/docs/data-stream.md) augmented with Kafka specific methods.
 * @extends DataStream
 */
class KafkaStream extends DataStream {

    /**
     * Creates a stream from kafka.
     *
     * @param {ZKConnectionOptions} connection Zookeeper connection string
     * @param {Object} [consumerOptions={}] Consumer options
     * @param {Object} streamOptions Options passed to scramjet
     */
    constructor(connection, options = {}, ...streamOptions) {
        super(...streamOptions);
        this._connected = false;
        this._kafka_options = {
            connection,
            payloads: [],
            options: options
        };
    }

    /**
     * Connection options from `kafka-node`
     *
     * @typedef ZKConnectionOptions
     * @param {String} connectionString: Zookeeper connection string, default localhost:2181/
     * @param {String} clientId: This is a user-supplied identifier for the client application, default kafka-node-client
     * @param {Object} zkOptions: Object, Zookeeper options, see node-zookeeper-client
     * @param {Object} noAckBatchOptions: Object, when requireAcks is disabled on Producer side we can define the batch properties, 'noAckBatchSize' in bytes and 'noAckBatchAge' in milliseconds. The default value is { noAckBatchSize: null, noAckBatchAge: null } and it acts as if there was no batch
     * @param {Object} sslOptions: Object, options to be passed to the tls broker sockets, ex. { rejectUnauthorized: false } (Kafka +0.9)
     */

    /**
     * Opens up connection to kafka and starts streaming.
     * @chainable
     */
    connect() {
        const {
            connection,
            payloads,
            options
        } = this._kafka_options;

        const client = getConnection(connection);
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
        this.options.payloads.push(...topics);
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

module.exports = {
    KafkaStream,
    /**
     * Consume a topic from kafka and return a new KafkaStream
     *
     * @param  {String} connection
     * @param  {Array.<String|Topic>}  [topics=[]] Topics to pull from Kafka
     * @chaiable
     */
    consume(connection, topics = []) {
        const ret = new KafkaStream(connection);
        ret.addTopics(topics);
        return ret.connect();
    },
    /**
     * Produces a topic in kafka and resolves when all items are pushed
     *
     * @param  {ZKConnectionOptions}  connection [description]
     * @param  {String|Topic}  topic      [description]
     * @return {Promise}            [description]
     */
    async produce(connection, topic) {
        const client = getConnection(connection);
        const producer = new ProducerStream(client);

        const ref = new DataStream();

        ref.timeBatch(40, 64)
            .map(messages => ({topic, messages}))
            .pipe(producer);

        return this.whenEnd();
    }
};

/**
 * @typedef Topic
 * @prop {String} topic topic name in kafka
 * @prop {Number} offset where to start reading
 * @prop {Number} partition kafka partition number
 */
