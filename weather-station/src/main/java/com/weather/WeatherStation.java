package com.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class WeatherStation {

    private static final String KAFKA_TOPIC = "weather-readings";
    private static final String KAFKA_SERVER =
        System.getenv().getOrDefault("KAFKA_SERVER", "127.0.0.1:9092");

    private final Random random = new Random();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final int stationId;
    private int sequenceNumber = 1;

    public WeatherStation(int stationId) {
        this.stationId = stationId;
    }

    public void start() throws Exception {

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                KAFKA_SERVER
        );

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(properties)) {

            while (true) {

                // 10% chance to drop the message
                if (random.nextInt(100) < 10) {

                    System.out.println(
                            "Station " + stationId +
                            " DROPPED message s_no=" +
                            sequenceNumber
                    );

                    sequenceNumber++;

                } else {

                    String json = createWeatherMessage();

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(
                                    KAFKA_TOPIC,
                                    String.valueOf(stationId),
                                    json
                            );

                    producer.send(record);

                    System.out.println(
                            "Station " + stationId +
                            " SENT: " + json
                    );

                    sequenceNumber++;
                }

                // One message attempt every second
                Thread.sleep(1000);
            }
        }
    }

    private String createWeatherMessage() throws Exception {

        Map<String, Object> message = new HashMap<>();

        message.put("station_id", stationId);
        message.put("s_no", sequenceNumber);

        message.put(
                "battery_status",
                generateBatteryStatus()
        );

        message.put(
                "status_timestamp",
                Instant.now().getEpochSecond()
        );

        Map<String, Integer> weather = new HashMap<>();

        weather.put(
                "humidity",
                random.nextInt(101)
        );

        weather.put(
                "temperature",
                50 + random.nextInt(71)
        );

        weather.put(
                "wind_speed",
                random.nextInt(101)
        );

        message.put("weather", weather);

        return objectMapper.writeValueAsString(message);
    }

    private String generateBatteryStatus() {

        int value = random.nextInt(100);

        if (value < 30) {
            return "low";
        } else if (value < 70) {
            return "medium";
        } else {
            return "high";
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {

            System.out.println(
                    "Usage: java WeatherStation <station_id>"
            );

            System.exit(1);
        }

        int stationId = Integer.parseInt(args[0]);

        WeatherStation station =
                new WeatherStation(stationId);

        station.start();
    }
}
