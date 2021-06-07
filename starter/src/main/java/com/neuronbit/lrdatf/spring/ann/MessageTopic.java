package com.neuronbit.lrdatf.spring.ann;

import com.neuronbit.lrdatf.spring.consumer.DuplicateMessageDetector;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageTopic {
    String name();

    Class<?> duplicateMessageDetector() default DuplicateMessageDetector.class;
}
