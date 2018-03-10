#!/usr/bin/env node

if (!process.env.KAFKA_URL) {
    console.log("No kafka string set, omitting test");
    process.exit(0);
}

const {Client, Producer} = require('kafka-node');
const scramjet = require("scramjet");

const client1 = new Client(process.env.KAFKA_URL, "scramjet-test1");
const client2 = new Client(process.env.KAFKA_URL, "scramjet-test2");
const client3 = new Client(process.env.KAFKA_URL, "scramjet-test3");

const producer = new Producer(client1);
const createTopics = () => new Promise((res, rej) => {
    producer.createTopics(["sj-test1", "sj-test2"], true, (err) => err ? rej(err) : res());
});

const ready = () => new Promise((res, rej) => {
    producer.on("ready", res);
    producer.on("error", rej);
});

(async () => {
    const {consume, augment} = require("../");
    console.error("Running test on " + process.env.KAFKA_URL);

    await ready();
    console.error("- Ready");
    await createTopics();
    console.error("- Created topics");

    await Promise.all([
        scramjet.fromArray([1,2,3,4])
            .map(a => ({a, b: a+a, c: a**a}))
            .each(console.log)
            .produceKafka(client1, "sj-test1")
            .then(
                () => console.log('- Produced stream')
            )
        ,
        augment(
            client2,
            "sj-test1",
            stream => stream.each(console.log).map(({a,c}) => Object.assign({x: c+a})),
            "sj-test2"
        )
            .then(
                () => console.log('- Augmented stream')
            )
        ,
        consume(client3, "sj-test2")
            .until(({a}) => a > 3)
            .each(x => console.error(" + ", x))
            .whenEnd()
            .then(() => console.log('- Consumed stream'))
    ]);

})()
    .then(
        () => (client1.close(), client2.close(), client3.close())
    ).catch(
        (e) => {
            console.error(e.stack);
            process.exit(127);
        }
    );
