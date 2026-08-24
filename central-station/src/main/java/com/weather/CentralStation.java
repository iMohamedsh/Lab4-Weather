package com.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CentralStation {

    private static final String KAFKA_SERVER =
        System.getenv().getOrDefault("KAFKA_SERVER", "127.0.0.1:9092");
    private static final String TOPIC = "weather-readings";

    private static final String DATABASE_URL =
        System.getenv().getOrDefault(
        "DATABASE_URL",
        "jdbc:postgresql://127.0.0.1:5432/weather"
        );

    private static final String DATABASE_USER = "weather";
    private static final String DATABASE_PASSWORD = "weather123";

    private static final ObjectMapper objectMapper =
            new ObjectMapper();

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_SERVER
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "central-station"
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        try (
                KafkaConsumer<String, String> consumer =
                        new KafkaConsumer<>(properties);

                Connection connection =
                        DriverManager.getConnection(
                                DATABASE_URL,
                                DATABASE_USER,
                                DATABASE_PASSWORD
                        )
        ) {

            consumer.subscribe(
                    Collections.singletonList(TOPIC)
            );

            System.out.println(
                    "Central Station started."
            );

            System.out.println(
                    "Connected to PostgreSQL."
            );

            System.out.println(
                    "Listening to weather-readings..."
            );

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(
                                Duration.ofMillis(1000)
                        );

                for (ConsumerRecord<String, String> record : records) {

                    saveWeatherReading(
                            connection,
                            record.value()
                    );
                }
            }

        } catch (Exception e) {

            System.err.println(
                    "Central Station error:"
            );

            e.printStackTrace();
        }
    }

    private static void saveWeatherReading(
            Connection connection,
            String message
    ) {

        String sql =
                "INSERT INTO weather_readings " +
                "(station_id, s_no, temperature, humidity, " +
                "wind_speed, status_timestamp, battery_status) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try {

            JsonNode root =
                    objectMapper.readTree(message);

            int stationId =
                    root.get("station_id").asInt();

            int sequenceNumber =
                    root.get("s_no").asInt();

            JsonNode weather =
                    root.get("weather");

            int temperature =
                    weather.get("temperature").asInt();

            int humidity =
                    weather.get("humidity").asInt();

            int windSpeed =
                    weather.get("wind_speed").asInt();

            long timestamp =
                    root.get("status_timestamp").asLong();

            String batteryStatus =
                    root.get("battery_status").asText();

            try (
                    PreparedStatement statement =
                            connection.prepareStatement(sql)
            ) {

                statement.setInt(1, stationId);
                statement.setInt(2, sequenceNumber);
                statement.setInt(3, temperature);
                statement.setInt(4, humidity);
                statement.setInt(5, windSpeed);
                statement.setLong(6, timestamp);
                statement.setString(7, batteryStatus);

                statement.executeUpdate();
            }

            System.out.println(
                    "Saved station " +
                    stationId +
                    " s_no=" +
                    sequenceNumber +
                    " humidity=" +
                    humidity +
                    " battery=" +
                    batteryStatus
            );

            if (batteryStatus.equalsIgnoreCase("low")) {

                System.out.println(
                        "LOW BATTERY ALERT: Station " +
                        stationId
                );
            }

        } catch (Exception e) {

            System.err.println(
                    "Could not save message:"
            );

            System.err.println(message);

            e.printStackTrace();
        }
    }
}
