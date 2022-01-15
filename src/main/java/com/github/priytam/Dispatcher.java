package com.github.priytam;

import static io.lettuce.core.SetArgs.Builder.ex;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.github.priytam.event.Event;
import com.github.priytam.event.EventEnvelope;
import io.lettuce.core.Range;
import io.lettuce.core.RedisException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dispatcher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);
    private final ScheduledThreadPoolExecutor dispatcherExecutor = new ScheduledThreadPoolExecutor(1);
    private final QueueSettings queueSettings;

    public Dispatcher(Subscription subscription, QueueSettings queueSettings) {
        this.queueSettings = queueSettings;


        if (queueSettings.isEnableScheduling()) {
            dispatcherExecutor.scheduleWithFixedDelay(this::dispatch, queueSettings.getSchedulingInterval(),
                    queueSettings.getSchedulingInterval(), NANOSECONDS);
        }

        if (queueSettings.getRefreshInterval() <= 0) {
            dispatcherExecutor.scheduleWithFixedDelay(subscription::refreshSubscriptions,
                    queueSettings.getRefreshInterval(), queueSettings.getRefreshInterval(), NANOSECONDS);
        }

    }

    void dispatch() {
        LOG.debug("delayed events dispatch started");
        try {
            String lock = tryLock();
            if (lock == null) {
                LOG.debug("unable to obtain lock for delayed events dispatch");
                return;
            }
            List<String> tasksForExecution = queueSettings.getDispatchCommands()
                    .zrangebyscore(queueSettings.getZsetName(),
                            Range.create(-1, System.currentTimeMillis()),
                            queueSettings.getSchedulingBatchSize());
            if (null == tasksForExecution) {
                return;
            }
            tasksForExecution.forEach(this::handleDelayedTask);
        } catch (RedisException e) {
            LOG.warn("Error during dispatch", e);
            queueSettings.getDispatchCommands().reset();
            throw e;
        } finally {
            unlock();
            LOG.debug("delayed events dispatch finished");
        }
    }

    private void handleDelayedTask(String key) {
        String rawEnvelope = queueSettings.getDispatchCommands().hget(queueSettings.getMetadataHset(), key);
        if (rawEnvelope == null) {
            if (queueSettings.getDispatchCommands().zrem(queueSettings.getZsetName(), key) > 0) {
                LOG.debug("key '{}' not found in HSET", key);
            }
            return;
        }
        EventEnvelope<? extends Event> currentEnvelope = queueSettings.getSerializerDeserializer()
                .deserialize(Event.class, rawEnvelope);
        EventEnvelope<? extends Event> nextEnvelope = EventEnvelope.nextAttempt(currentEnvelope);
        queueSettings.getDispatchCommands().multi();
        if (nextEnvelope.getAttempt() < queueSettings.getRetryAttempts()) {
            queueSettings.getDispatchCommands().zadd(queueSettings.getZsetName(),
                    nextAttemptTime(nextEnvelope.getAttempt()), key);
            queueSettings.getDispatchCommands().hset(queueSettings.getMetadataHset(), key,
                    queueSettings.getSerializerDeserializer().serialize(nextEnvelope));
        } else {
            queueSettings.getDispatchCommands().zrem(queueSettings.getZsetName(), key);
            queueSettings.getDispatchCommands().hdel(queueSettings.getMetadataHset(), key);
        }
        queueSettings.getDispatchCommands().lpush(toQueueName(currentEnvelope.getType()),
                queueSettings.getSerializerDeserializer().serialize(currentEnvelope));

        queueSettings.getDispatchCommands().exec();
        LOG.debug("dispatched event [{}]", currentEnvelope);
    }

    private String tryLock() {
        return queueSettings.getDispatchCommands().set(queueSettings.getLockKey(),
                "value", ex(queueSettings.getLockTimeout().toMillis() * 1000).nx());
    }

    private void unlock() {
        try {
            queueSettings.getDispatchCommands().del(queueSettings.getLockKey());
        } catch (RedisException e) {
            queueSettings.getDispatchCommands().reset();
        }
    }

    private String toQueueName(Class<? extends Event> cls) {
        return queueSettings.getDataSetPrefix() + cls.getSimpleName().toLowerCase();
    }


    private long nextAttemptTime(int attempt) {
        return System.currentTimeMillis() + nextDelay(attempt) * 1000;
    }


    private long nextDelay(int attempt) {
        if (attempt < 10) { // first 10 attempts each 10 seconds
            return 10L;
        }

        if (attempt < 10 + 10) { // next 10 attempts each minute
            return 60L;
        }

        return 60L * 24; // next 50 attempts once an hour
    }


    @Override
    public void close() throws IOException {
        dispatcherExecutor.shutdownNow();
        queueSettings.getDispatchCommands().getStatefulConnection().close();
    }
}

