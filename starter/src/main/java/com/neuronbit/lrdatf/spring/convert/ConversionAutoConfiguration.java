package com.neuronbit.lrdatf.spring.convert;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.support.GenericConversionService;

@Configuration
@ConditionalOnBean(type = "org.springframework.core.convert.support.GenericConversionService")
@ConditionalOnMissingClass("org.springframework.boot.convert.StringToDurationConverter")
public class ConversionAutoConfiguration {

    @Autowired
    public void addConverter(GenericConversionService genericConversionService) {
        genericConversionService.addConverter(new StringToDurationConverter());
        genericConversionService.addConverter(new DurationToStringConverter());
        genericConversionService.addConverter(new NumberToDurationConverter());
        genericConversionService.addConverter(new DurationToNumberConverter());
    }
}
