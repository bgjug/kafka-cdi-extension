package bg.jug.cdi.kafka;

import org.apache.deltaspike.core.util.metadata.builder.AnnotatedTypeBuilder;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.event.TransactionPhase;
import javax.enterprise.inject.spi.*;
import javax.enterprise.util.AnnotationLiteral;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaCDIExtension implements Extension {

    private Set<AnnotatedMethod<?>> consumeMethods = new HashSet<>();
    private KafkaUpdatesListener kafkaUpdatesListener;

    public void addKafkaObjectProducers(@Observes BeforeBeanDiscovery bbd, BeanManager beanManager) {
        System.out.println("[Kafka ext] Adding Kafka Producer Factory");
        bbd.addAnnotatedType(beanManager.createAnnotatedType(KafkaProducerFactory.class));
        bbd.addAnnotatedType(beanManager.createAnnotatedType(KafkaUpdatesListener.class));
        bbd.addQualifier(Topic.class);
    }

    public void collectConsumerMethods(@Observes ProcessAnnotatedType<?> pat) {
        pat.getAnnotatedType().getMethods().stream()
                .filter(m -> m.isAnnotationPresent(Consumes.class))
                .forEach(m -> handleConsumerMethod(pat, m));
    }

    private void handleConsumerMethod(ProcessAnnotatedType<?> pat, AnnotatedMethod<?> m) {
        System.out.println("[Kafka ext] Handling consumer method " + m.getJavaMember().getName());
        consumeMethods.add(m);
        AnnotatedParameter<?> recordsParameter = m.getParameters().get(0);
        String topic = m.getAnnotation(Consumes.class).topic();
        AnnotatedTypeBuilder annotatedTypeBuilder = new AnnotatedTypeBuilder().readFromType(m.getDeclaringType());
        annotatedTypeBuilder.addToParameter(recordsParameter, new ObservesLiteral());
        annotatedTypeBuilder.addToParameter(recordsParameter, new TopicLiteral(topic));
        pat.setAnnotatedType(annotatedTypeBuilder.create());
    }

    public void listObservers(@Observes ProcessObserverMethod pom) {
        ObserverMethod observerMethod = pom.getObserverMethod();
        System.out.println("[Kafka ext] Found observer method: " + observerMethod.getBeanClass()
         + "(" + observerMethod.getObservedQualifiers().toString() + " " + observerMethod.getObservedType() + ")");
    }

    public void startMonitoring(@Observes AfterDeploymentValidation adv, BeanManager bm) {
        KafkaUpdatesListener kafkaUpdatesListener = initializeListener(bm);
        startListener(kafkaUpdatesListener);
        this.kafkaUpdatesListener = kafkaUpdatesListener;
    }

    private KafkaUpdatesListener initializeListener(BeanManager bm) {
        System.out.println("[Kafka ext] Initializing kafka updates listener");
        Bean<KafkaUpdatesListener> listenerBean = (Bean<KafkaUpdatesListener>) bm.getBeans(KafkaUpdatesListener.class).iterator().next();
        CreationalContext<KafkaUpdatesListener> creationalContext = bm.createCreationalContext(listenerBean);
        KafkaUpdatesListener reference = (KafkaUpdatesListener) bm.getReference(listenerBean, KafkaUpdatesListener.class, creationalContext);

        consumeMethods.forEach(m -> addTopicToListen(m, reference));
        return reference;
    }

    private void addTopicToListen(AnnotatedMethod<?> m, KafkaUpdatesListener reference) {
        Consumes annotation = m.getAnnotation(Consumes.class);
        System.out.println("[Kafka ext] Adding topic " + annotation.topic() + " to listen");
        reference.addTopicToListen(KafkaConsumerFactory.createConsumer(
                    annotation.bootstrapServers(), annotation.groupId(),
                    annotation.keyType(), annotation.valueType(), annotation.topic()),
                annotation.topic());
    }

    private void startListener(KafkaUpdatesListener kafkaUpdatesListener) {
        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
        } catch (NamingException e) {
            executorService = new ThreadPoolExecutor(16, 16, 10, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
        }

        System.out.println("[Kafka ext] Starting listener via " + executorService);
        // submit the consumer
        executorService.submit(kafkaUpdatesListener);
    }


    public void stopListener(@Observes BeforeShutdown bsh) {
        kafkaUpdatesListener.stopPolling();
    }

    private static class ObservesLiteral extends AnnotationLiteral<Observes> implements Observes {

        private Reception reception;
        private TransactionPhase transactionPhase;

        public ObservesLiteral() {
            this(Reception.ALWAYS, TransactionPhase.IN_PROGRESS);
        }

        public ObservesLiteral(Reception reception, TransactionPhase transactionPhase) {
            this.reception = reception;
            this.transactionPhase = transactionPhase;
        }

        @Override
        public Reception notifyObserver() {
            return reception;
        }

        @Override
        public TransactionPhase during() {
            return transactionPhase;
        }
    }
}
