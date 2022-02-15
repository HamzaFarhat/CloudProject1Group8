//const Kafka = require("kafkajs").Kafka
const {Kafka} = require("kafkajs")
const topic = "lab2"

run();
async function run(){
    try
    {
        const kafka = new Kafka({
            "brokers" :["localhost:9093"]
        })

        const admin = kafka.admin();
        console.log("Connecting.....")
        await admin.connect()
        console.log("Connected!")
        await admin.createTopics({
            "topics": [{
                "topic" : topic,
                "numPartitions": 3
            }]
        })
        console.log("Created Successfully!")
        await admin.disconnect();
    }
    catch(ex)
    {
        console.error(`Something bad happened ${ex}`)
    }
    finally{
        process.exit(0);
    }


}