package com.github.lisama569.asyncjobqueuejava.job;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * jobType -> JobHandler 注册表。Spring 容器中的所有 JobHandler Bean 自动注册。
 */
@Component
public class JobHandlerRegistry {
    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public JobHandlerRegistry(List<JobHandler> handlerBeans) {
        for (JobHandler h : handlerBeans) {
            register(h);
        }
    }

    public final void register(JobHandler handler) {
        JobHandler old = handlers.put(handler.type(), handler);
        if (old != null && old != handler) {
            throw new IllegalStateException("重复的 jobType: " + handler.type());
        }
    }

    public Optional<JobHandler> find(String jobType) {
        return Optional.ofNullable(handlers.get(jobType));
    }
}
