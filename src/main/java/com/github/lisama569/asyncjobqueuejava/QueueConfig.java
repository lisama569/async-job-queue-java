package com.github.lisama569.asyncjobqueuejava;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobQueueProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(JobQueueProperties.class)
public class QueueConfig {
}
