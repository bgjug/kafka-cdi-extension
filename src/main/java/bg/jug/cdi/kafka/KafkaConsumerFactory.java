package bg.jug.cdi.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerFactory {

    public static <K,V> KafkaConsumer<K, V> createConsumer(String bootstrapServers, String groupId,
                                                    Class<K> keyDeserializer, Class<V> valueDeserializer,
                                                    String topic) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", bootstrapServers);
        consumerProperties.setProperty("key.deserializer", getSerializerFor(keyDeserializer));
        consumerProperties.setProperty("value.deserializer", getSerializerFor(valueDeserializer));
        consumerProperties.setProperty("group.id", groupId);
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        System.out.println("Created Kafka consumer");
        return kafkaConsumer;
    }

    private static String getSerializerFor(Class aClass) {
        if (aClass.equals(String.class)) {
            return StringDeserializer.class.getCanonicalName();
        } else if (aClass.equals(Integer.class) || aClass.equals(int.class)) {
            return IntegerDeserializer.class.getCanonicalName();
        }
        throw new IllegalArgumentException();
    }

}

