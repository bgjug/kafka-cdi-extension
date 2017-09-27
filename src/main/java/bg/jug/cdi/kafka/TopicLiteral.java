package bg.jug.cdi.kafka;

import javax.enterprise.util.AnnotationLiteral;

public class TopicLiteral extends AnnotationLiteral<Topic> implements Topic {

    private final String value;

    public TopicLiteral(String value) {
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }
}
