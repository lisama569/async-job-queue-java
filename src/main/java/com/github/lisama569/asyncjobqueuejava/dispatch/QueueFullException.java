package com.github.lisama569.asyncjobqueuejava.dispatch;

/**
 * 队列容量打满时的背压异常（提交方应明确感知）。
 */
public class QueueFullException extends RuntimeException {
    public QueueFullException(String message) {
        super(message);
    }
}
