package org.kafka.com;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SampleKafkaProducer extends Thread{
    private final KafkaProducer<Integer, String> producer;
    private  final String topic;
    private final Boolean isAsync;
    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String CLIENT_ID = "producer-1";
    private static final String TOPIC = "ECOMMERCE_NEW_ORDER";
    private static final int MESSAGES = 100;

    public SampleKafkaProducer(String topic, Boolean isAsync){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        prop.put("client.id", CLIENT_ID);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(prop);
        this.topic = topic;
        this.isAsync = isAsync;
    }
    public static void main(String[] args){
        boolean isAsync = false;
        SampleKafkaProducer producer = new SampleKafkaProducer(TOPIC, isAsync);
        producer.start();
    }

    public void run(){
        int messageNo = 1;
        while (messageNo < MESSAGES){
            String messageStr = "This is Message number: " + messageNo;
            long starTime = System.currentTimeMillis();

            if (isAsync){
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr), new MessageCallBack(starTime, messageNo, messageStr));
            } else {
                try {
                    producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();
                    System.out.println("Send message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e){
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }

}

class MessageCallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;

    public MessageCallBack(long startTime, int key, String message){
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception){
        long elapsedTime = System.currentTimeMillis() - startTime;

        if (metadata != null){
            System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                 "), " + "offset( " + metadata.offset() + ") in " + elapsedTime + "ms");
        } else {
            exception.printStackTrace();
        }
    }
}




