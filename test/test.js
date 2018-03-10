#!/usr/bin/env node

if (!process.env.KAFKA_URL) {
    console.log("No kafka string set, omitting test");
    process.exit(0);
}

const kafkaLogging = require('kafka-node/logging');
kafkaLogging.setLoggerProvider(() => console);

const scramjet = require("scramjet");
const {consume, augment} = require("../");

const connection = process.env.KAFKA_DIRECT ? {
    kafkaOptions: {kafkaHost: process.env.KAFKA_URL}
} : {
    zkOptions: {connectionString: process.env.KAFKA_URL}
};

(async () => {
    console.error("Running test on ", connection);

    await Promise.all([
        scramjet.fromArray([1,2,3,4])
            .map(a => ({a, b: a+a, c: a**a}))
            .each(console.log)
            .JSONStringify()
            .produceKafka(connection, "sj-test1")
            .then(
                () => console.log('- Produced stream')
            )
        ,
        augment(
            connection,
            "sj-test1",
            stream => stream.each(console.log).map(({a,c}) => Object.assign({x: c+a})),
            "sj-test2"
        )
            .then(
                () => console.log('- Augmented stream')
            )
        ,
        consume(connection, "sj-test2")
            .until(({a}) => a > 3)
            .each(x => console.error(" + ", x))
            .whenEnd()
            .then(() => console.log('- Consumed stream'))
    ]);

})()
    .catch(
        (e) => {
            console.error(e.stack);
            process.exit(127);
        }
    );
