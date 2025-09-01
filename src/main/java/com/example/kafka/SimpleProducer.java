package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SimpleProducer {
    
    public static void main(String[] args) {
        // Configuration
        var props = new Properties();
        props.put("bootstrap.servers", "pkc-z1o60.europe-west1.gcp.confluent.cloud:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"I2VWZVM4TOZAT4GR\" " +
            "password=\"cflt+JpqkGbJilKMfB1c4IqHtW0AgCh2P6Ts0oPLKD2+FKJUSuuNe+OzU4LCs3+g\";");
        
        // Créer producer et envoyer message
        try (var producer = new KafkaProducer<String, String>(props)) {
            
            var message = args.length > 0 ? String.join(" ", args) : "Hello Kafka!";
            var record = new ProducerRecord<>("hq.dev.rdv.refined.actors.v1", "key", message);
            System.out.println("🏷️ Ajout des headers...");
            record.headers().add("eventSource", "SALESFORCE".getBytes(StandardCharsets.UTF_8));
            record.headers().add("operation", "CREATE".getBytes(StandardCharsets.UTF_8));
            record.headers().add("countryCode", "FR".getBytes(StandardCharsets.UTF_8));
            record.headers().add(new RecordHeader("eventId", "123456".getBytes(StandardCharsets.UTF_8)));
            System.out.println("📤 Envoi: " + message);
            
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("✅ Envoyé! Partition: " + metadata.partition() + 
                                     ", Offset: " + metadata.offset());
                } else {
                    System.err.println("❌ Erreur: " + exception.getMessage());
                }
            });
            
            producer.flush(); // Attendre l'envoi
            
        } catch (Exception e) {
            System.err.println("❌ Erreur: " + e.getMessage());
        }
    }
}