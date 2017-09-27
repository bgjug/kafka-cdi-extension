package bg.jug.cdi.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.util.Properties;

@ApplicationScoped
public class KafkaProducerFactory {

    @Produces
    public <K,V> KafkaProducer<K, V> createProducer(InjectionPoint injectionPoint) {
        KafkaProducerConfig annotation = injectionPoint.getAnnotated().getAnnotation(KafkaProducerConfig.class);
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", annotation.bootstrapServers());
        producerProperties.setProperty("key.serializer", getSerializerFor(annotation.keyType()));
        producerProperties.setProperty("value.serializer", getSerializerFor(annotation.valueType()));
        return new KafkaProducer<>(producerProperties);
    }

    private String getSerializerFor(Class aClass) {
        if (aClass.equals(String.class)) {
            return StringSerializer.class.getCanonicalName();
        } else if (aClass.equals(Integer.class) || aClass.equals(int.class)) {
            return IntegerSerializer.class.getCanonicalName();
        }
        throw new IllegalArgumentException();
    }
}
