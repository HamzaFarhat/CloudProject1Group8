const {Kafka} = require("kafkajs")
const topic = "lab2"

run();
async function run(){
    try
    {
        const kafka = new Kafka({
            "brokers" :["localhost:9093"]
        })

        const consumer = kafka.consumer({"groupId": "test"})
        console.log("Connecting.....")
        await consumer.connect()
        console.log("Connected!")

        await consumer.subscribe({
            "topic": topic,
            "fromBeginning": true
        })

        await consumer.run({
            "eachMessage": async result => {
                console.log(`RVD Msg ${result.message.value} on partition ${result.partition}`)
            }
        })


    }
    catch(ex)
    {
        console.error(`Something bad happened ${ex}`)
    }
    finally{

    }


}