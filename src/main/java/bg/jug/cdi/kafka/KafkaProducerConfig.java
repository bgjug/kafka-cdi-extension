package bg.jug.cdi.kafka;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({ TYPE, METHOD, FIELD, PARAMETER })
@Retention(RUNTIME)
public @interface KafkaProducerConfig {

    Class keyType() default String.class;
    Class valueType() default String.class;
    String bootstrapServers() default "localhost:9092";

}
