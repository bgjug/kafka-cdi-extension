package bg.jug.cdi.kafka;

import javax.enterprise.inject.spi.AnnotatedMethod;

public class KafkaConsumerProcessor {

    private final Class<?> consumerClass;
    private final AnnotatedMethod<?> consumerMethod;
    private final String topic;

    public KafkaConsumerProcessor(Class<?> clazz, AnnotatedMethod<?> consumerMethod, String topic) {
        this.consumerClass = clazz;
        this.consumerMethod = consumerMethod;
        this.topic = topic;
    }

    public Class<?> getConsumerClass() {
        return consumerClass;
    }

    public AnnotatedMethod<?> getConsumerMethod() {
        return consumerMethod;
    }

    public String getTopic() {
        return topic;
    }
}
