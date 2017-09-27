package bg.jug.cdi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaCDIExtension implements Extension {

    private Set<KafkaConsumerProcessor> consumerProcessors = new HashSet<>();
    private KafkaUpdatesListener kafkaUpdatesListener;
    private List<Exception> errors = new ArrayList<>();

    public void addKafkaObjectProducers(@Observes BeforeBeanDiscovery bbd, BeanManager beanManager) {
        System.out.println("[Kafka ext] Adding extension types");
        bbd.addAnnotatedType(beanManager.createAnnotatedType(KafkaProducerFactory.class));
        bbd.addAnnotatedType(beanManager.createAnnotatedType(KafkaUpdatesListener.class));
    }

    public void collectConsumerMethods(@Observes @WithAnnotations(Consumes.class) ProcessAnnotatedType<?> pat) {
        System.out.println("[Kafka ext] Collecting consumer processors");
        pat.getAnnotatedType().getMethods().stream()
                .filter(m -> m.isAnnotationPresent(Consumes.class))
                .forEach(m -> handleConsumerMethod(pat.getAnnotatedType(), m));
    }

    private void handleConsumerMethod(AnnotatedType<?> clazz, AnnotatedMethod<?> annotatedMethod) {
        System.out.println("[Kafka ext] Handling consumer method " + annotatedMethod.getJavaMember().getName());
        List<? extends AnnotatedParameter<?>> parameters = annotatedMethod.getParameters();
        if (parameters.size() != 1) {
            errors.add(new IllegalArgumentException("@Consume methods should only have one parameter"));
        } else {
            consumerProcessors.add(new KafkaConsumerProcessor(clazz.getJavaClass(), annotatedMethod,
                    annotatedMethod.getAnnotation(Consumes.class).topic()));
        }
    }

    public void startMonitoring(@Observes AfterDeploymentValidation adv, BeanManager bm) {
        if (!errors.isEmpty()) {
            errors.forEach(adv::addDeploymentProblem);
        } else {
            KafkaUpdatesListener kafkaUpdatesListener = initializeListener(bm);
            startListener(kafkaUpdatesListener);
            this.kafkaUpdatesListener = kafkaUpdatesListener;
        }
    }

    private KafkaUpdatesListener initializeListener(BeanManager bm) {
        System.out.println("[Kafka ext] Initializing kafka updates listener");
        Class<KafkaUpdatesListener> clazz = KafkaUpdatesListener.class;
        KafkaUpdatesListener reference = (KafkaUpdatesListener) createBean(bm, clazz);

        consumerProcessors.forEach(m -> addTopicToListen(m, reference));
        return reference;
    }

    static Object createBean(BeanManager bm, Class<?> clazz) {
        Set<Bean<?>> listenerBeans = bm.getBeans(clazz);
        Bean<?> listenerBean = bm.resolve(listenerBeans);
        CreationalContext<?> creationalContext = bm.createCreationalContext(listenerBean);
        return bm.getReference(listenerBean, clazz, creationalContext);
    }

    private void addTopicToListen(KafkaConsumerProcessor processor, KafkaUpdatesListener listener) {
        Consumes annotation = processor.getConsumerMethod().getAnnotation(Consumes.class);
        System.out.println("[Kafka ext] Adding topic " + annotation.topic() + " to listen");
        listener.addConsumerProcessor(KafkaConsumerFactory.createConsumer(
                    annotation.bootstrapServers(), annotation.groupId(),
                    annotation.keyType(), annotation.valueType(), annotation.topic()),
                processor);
    }

    private void startListener(KafkaUpdatesListener kafkaUpdatesListener) {
        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
        } catch (NamingException e) {
            executorService = new ThreadPoolExecutor(16, 16, 10, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
        }

        System.out.println("[Kafka ext] Starting listener via " + executorService);
        executorService.submit(kafkaUpdatesListener);
    }


    public void stopListener(@Observes BeforeShutdown bsh) {
        kafkaUpdatesListener.stopPolling();
    }
}
