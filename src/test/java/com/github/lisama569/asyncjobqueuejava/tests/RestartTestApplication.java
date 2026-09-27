package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.Application;
import com.github.lisama569.asyncjobqueuejava.dispatch.DefaultJobDispatcher;
import com.github.lisama569.asyncjobqueuejava.dispatch.DispatcherShutdown;
import com.github.lisama569.asyncjobqueuejava.dispatch.RetryScheduler;
import com.github.lisama569.asyncjobqueuejava.dispatch.StartupRecovery;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * 重启恢复测试专用 Spring Boot 应用：
 * 排除生产环境的调度器、重试扫描、启动恢复与关闭钩子，
 * 这些“进程内”组件改由 {@link ProcessRestartSimulator} 显式创建/销毁，
 * 以精确模拟进程崩溃与重启。
 */
@SpringBootApplication
@ComponentScan(
        basePackageClasses = Application.class,
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.ASSIGNABLE_TYPE,
                classes = {
                        DefaultJobDispatcher.class,
                        DispatcherShutdown.class,
                        RetryScheduler.class,
                        StartupRecovery.class
                }))
public class RestartTestApplication {
}
