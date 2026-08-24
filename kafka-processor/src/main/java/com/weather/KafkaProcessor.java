package com.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaProcessor {

    private static final String KAFKA_SERVER =
        System.getenv().getOrDefault("KAFKA_SERVER", "127.0.0.1:9092");

    private static final String INPUT_TOPIC = "weather-readings";
    private static final String OUTPUT_TOPIC = "rain-alerts";

    private static final String GROUP_ID = "rain-alert-processor";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties consumerProperties = new Properties();

        consumerProperties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_SERVER
        );

        consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                GROUP_ID
        );

        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        consumerProperties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        Properties producerProperties = new Properties();

        producerProperties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_SERVER
        );

        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        try (
                KafkaConsumer<String, String> consumer =
                        new KafkaConsumer<>(consumerProperties);

                KafkaProducer<String, String> producer =
                        new KafkaProducer<>(producerProperties)
        ) {

            consumer.subscribe(
                    Collections.singletonList(INPUT_TOPIC)
            );

            System.out.println(
                    "Kafka Processor started..."
            );

            System.out.println(
                    "Listening to topic: " + INPUT_TOPIC
            );

            System.out.println(
                    "Sending alerts to: " + OUTPUT_TOPIC
            );

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    processWeatherMessage(
                            record,
                            producer
                    );
                }
            }

        } catch (Exception e) {

            System.err.println(
                    "Kafka Processor error:"
            );

            e.printStackTrace();
        }
    }

    private static void processWeatherMessage(
            ConsumerRecord<String, String> record,
            KafkaProducer<String, String> producer
    ) {

        try {

            String message = record.value();

            JsonNode root =
                    objectMapper.readTree(message);

            int stationId =
                    root.get("station_id").asInt();

            int sequenceNumber =
                    root.get("s_no").asInt();

            int humidity =
                    root.get("weather")
                            .get("humidity")
                            .asInt();

            System.out.println(
                    "Received station " +
                    stationId +
                    " s_no=" +
                    sequenceNumber +
                    " humidity=" +
                    humidity
            );

            /*
             * Rain condition:
             * humidity > 70%
             */
            if (humidity > 70) {

                String alertMessage =
                        createRainAlert(
                                stationId,
                                sequenceNumber,
                                humidity
                        );

                ProducerRecord<String, String> alert =
                        new ProducerRecord<>(
                                OUTPUT_TOPIC,
                                String.valueOf(stationId),
                                alertMessage
                        );

                producer.send(alert);

                System.out.println(
                        "RAIN ALERT: " +
                        alertMessage
                );
            }

        } catch (Exception e) {

            System.err.println(
                    "Could not process message: " +
                    record.value()
            );

            e.printStackTrace();
        }
    }

    private static String createRainAlert(
            int stationId,
            int sequenceNumber,
            int humidity
    ) throws Exception {

        var alert =
                objectMapper.createObjectNode();

        alert.put(
                "station_id",
                stationId
        );

        alert.put(
                "s_no",
                sequenceNumber
        );

        alert.put(
                "humidity",
                humidity
        );

        alert.put(
                "alert",
                "Rain detected"
        );

        return objectMapper.writeValueAsString(
                alert
        );
    }
}
