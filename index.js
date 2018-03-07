const {KafkaClient, ConsumerStream, ProducerStream} = require("kafka-node");
const scramjet = require("scramjet");

const getConnection = (conn) => {
    if (conn instanceof KafkaClient) {
        return conn;
    } else {
        return new KafkaClient(conn);
    }
};

module.exports = scramjet.plugin({
    DataStream: {
        async kafkaConsume(topic, connection = this._options.kafkaConnection) {
            const client = getConnection(connection);
            const consumer = new ConsumerStream(client);

            return consumer.pipe(this._selfInstance());
        },
        async kafkaPublish(topic, connection = this._options.kafkaConnection) {
            const client = getConnection(connection);
            const producer = new ProducerStream(client);

            const ref = this
                .timeBatch(40, 64)
                .map(messages => ({topic, messages}));

            ref.pipe(producer);

            return this.whenEnd();
        }
    }
});
