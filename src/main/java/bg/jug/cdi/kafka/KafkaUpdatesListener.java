package bg.jug.cdi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class KafkaUpdatesListener implements Runnable {

    private boolean continuePolling = true;

    private Map<KafkaConsumer<?, ?>, String> consumers = new HashMap<>();

    @Inject
    private BeanManager beanManager;

    @Override
    public void run() {
        System.out.println("[Kafka ext] Polling started");
        while (continuePolling) {
            consumers.forEach((key, value) -> {
                Iterable<? extends ConsumerRecord<?, ?>> records = key.poll(100).records(value);
                records.forEach(r -> {
                    TopicLiteral topicLiteral = new TopicLiteral(value);
                    System.out.println("Firing event with type: " + r.getClass() + " and qualifier" + topicLiteral);
                    beanManager.fireEvent(r, topicLiteral);
                });
            });

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public <K, V> void addTopicToListen(KafkaConsumer<K, V> consumer, String topic) {
        consumers.putIfAbsent(consumer, topic);
    }

    public void stopPolling() {
        continuePolling = false;
    }
}
