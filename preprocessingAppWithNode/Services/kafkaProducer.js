import {Kafka,logLevel} from 'kafkajs';

//const KAFKA_BROKER_ADDRESS = process.env.KAFKA_BROKER_ADDRESS

const KAFKA_BROKER_ADDRESS = 'kafka:9092'

const kafka = new Kafka({brokers:[KAFKA_BROKER_ADDRESS],logLevel:logLevel.ERROR})

const producer = kafka.producer()

await producer.connect()
export const kafkaProducer = async (data) => {
    await producer.send({
      topic: process.env.KAFKA_TOPIC,
      messages: [{ value: JSON.stringify(data) }]
    });
  };