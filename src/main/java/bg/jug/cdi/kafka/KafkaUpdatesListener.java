package bg.jug.cdi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class KafkaUpdatesListener implements Runnable {

    private boolean continuePolling = true;

    private Map<KafkaConsumer<?, ?>, KafkaConsumerProcessor> consumers = new HashMap<>();

    @Inject
    private BeanManager beanManager;

    @Override
    public void run() {
        System.out.println("[Kafka ext] Polling started");
        while (continuePolling) {
            consumers.forEach((key, value) -> key.poll(100)
                    .records(value.getTopic())
                    .forEach(r -> consumeRecord(r, value)));

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("[Kafka ext] Polling stopped");
    }

    private void consumeRecord(ConsumerRecord<?, ?> r, KafkaConsumerProcessor value) {
        Object reference = KafkaCDIExtension.createBean(beanManager, value.getConsumerClass());
        try {
            value.getConsumerMethod().getJavaMember().invoke(reference, r);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void addConsumerProcessor(KafkaConsumer<?, ?> consumer, KafkaConsumerProcessor processor) {
        consumers.putIfAbsent(consumer, processor);
    }

    public void stopPolling() {
        continuePolling = false;
    }
}
