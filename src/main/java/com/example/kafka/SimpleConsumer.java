package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) {
        var props = new Properties();
        props.put("bootstrap.servers", "pkc-z1o60.europe-west1.gcp.confluent.cloud:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "SA-salesforce-cdc-actors-client-MFS-debug_group");
        props.put("auto.offset.reset", "earliest");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"I2VWZVM4TOZAT4GR\" " +
                "password=\"cflt+JpqkGbJilKMfB1c4IqHtW0AgCh2P6Ts0oPLKD2+FKJUSuuNe+OzU4LCs3+g\";");

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("hq.dev.rdv.refined.actors.v1"));
            System.out.println("🎧 Consumer démarré - En attente de messages...");
            System.out.println("💡 Appuyez sur Ctrl+C pour arrêter");

            while (true) {
                var records = consumer.poll(Duration.ofMillis(1000));

                for (var record : records) {
                    if (hasHeader(record.headers(), "eventSource", "SALESFORCE")) {
                        
                        System.out.println("✅ Message de SALESFORCE - Traitement en cours...");
                        System.out.println("--- Headers ---");
                        for (Header header : record.headers()) {
                            System.out.printf("  %s: %s%n", header.key(), new String(header.value(), StandardCharsets.UTF_8));
                        }
                        System.out.println("---------------");
                        
                        // Traitement et affichage du message
                        System.out.printf("""
                                 📨 Message reçu:
                                   Topic: %s, Partition: %d, Offset: %d
                                   Key: %s
                                   Value: %s
                                """,
                                record.topic(), record.partition(), record.offset(),
                                record.key(), record.value()
                        );

                    } else {
                        // Si le header ne correspond pas, on ignore le message
                        System.out.printf("❌ Message ignoré (Offset: %d) - Source non-SALESFORCE.%n", record.offset());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Erreur: " + e.getMessage());
        }
    }

    /**
     * Vérifie si un message contient un header avec une clé et une valeur spécifiques.
     * @param headers Les headers du message.
     * @param key La clé du header à rechercher.
     * @param value La valeur du header à rechercher.
     * @return true si le header est trouvé, false sinon.
     */
    public static boolean hasHeader(Headers headers, String key, String value) {
        for (Header header : headers) {
            if (header.key().equals(key) && new String(header.value(), StandardCharsets.UTF_8).equals(value)) {
                return true;
            }
        }
        return false;
    }
}