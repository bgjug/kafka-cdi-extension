package bg.jug.cdi.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Consumes {

    String topic();
    Class keyType() default String.class;
    Class valueType() default String.class;
    String bootstrapServers() default "localhost:9092";
    String groupId() default "test";

}
