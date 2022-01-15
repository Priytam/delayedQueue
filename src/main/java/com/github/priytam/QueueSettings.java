package com.github.priytam;

import com.github.priytam.context.EventContextHandler;
import com.github.priytam.metrics.Metrics;
import io.lettuce.core.Limit;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import reactor.core.scheduler.Scheduler;

public final class QueueSettings {
    private final boolean enableScheduling;
    private final long schedulingInterval;
    private final long refreshInterval;
    private final SerializerDeserializer serializerDeserializer;
    private final RedisClient client;
    private final Metrics metrics;
    private final Scheduler handlerScheduler;
    private final int retryAttempts;
    private final Duration lockTimeout;
    private final Duration pollingTimeout;
    private final EventContextHandler contextHandler;
    private final String dataSetPrefix;
    private final Limit schedulingBatchSize;
    private final String zsetName;
    private final String lockKey;
    private final String metadataHset;
    private final RedisCommands<String, String> dispatchCommands;
    private final RedisCommands<String, String> metricsCommands;
    private final RedisReactiveCommands<String, String> reactiveCommands;


    public QueueSettings(boolean enableScheduling,
                         long schedulingInterval,
                         long refreshInterval,
                         SerializerDeserializer serializerDeserializer,
                         RedisClient client,
                         EventContextHandler contextHandler,
                         Duration pollingTimeout,
                         Duration lockTimeout,
                         int retryAttempts,
                         Scheduler handlerScheduler,
                         Metrics metrics,
                         String dataSetPrefix,
                         Limit schedulingBatchSize,
                         String zsetName,
                         String lockKey,
                         String metadataHset,
                         RedisReactiveCommands<String, String> reactiveCommands,
                         RedisCommands<String, String> dispatchCommands,
                         RedisCommands<String, String> metricsCommands) {
        this.enableScheduling = enableScheduling;
        this.schedulingInterval = schedulingInterval;
        this.refreshInterval = refreshInterval;
        this.serializerDeserializer = serializerDeserializer;
        this.client = client;
        this.contextHandler = contextHandler;
        this.pollingTimeout = pollingTimeout;
        this.lockTimeout = lockTimeout;
        this.retryAttempts = retryAttempts;
        this.handlerScheduler = handlerScheduler;
        this.metrics = metrics;
        this.dataSetPrefix = dataSetPrefix;
        this.schedulingBatchSize = schedulingBatchSize;
        this.zsetName = zsetName;
        this.lockKey = lockKey;
        this.metadataHset = metadataHset;
        this.dispatchCommands = dispatchCommands;
        this.reactiveCommands = reactiveCommands;
        this.metricsCommands = metricsCommands;
    }

    public SerializerDeserializer getSerializerDeserializer() {
        return serializerDeserializer;
    }

    public RedisClient getClient() {
        return client;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public Scheduler getHandlerScheduler() {
        return handlerScheduler;
    }

    public int getRetryAttempts() {
        return retryAttempts;
    }

    public Duration getLockTimeout() {
        return lockTimeout;
    }

    public Duration getPollingTimeout() {
        return pollingTimeout;
    }

    public EventContextHandler getContextHandler() {
        return contextHandler;
    }

    public String getDataSetPrefix() {
        return dataSetPrefix;
    }

    public Limit getSchedulingBatchSize() {
        return schedulingBatchSize;
    }

    public String getZsetName() {
        return zsetName;
    }

    public String getLockKey() {
        return lockKey;
    }

    public String getMetadataHset() {
        return metadataHset;
    }

    public RedisCommands<String, String> getDispatchCommands() {
        return dispatchCommands;
    }

    public RedisCommands<String, String> getMetricsCommands() {
        return metricsCommands;
    }

    public RedisReactiveCommands<String, String> getReactiveCommands() {
        return reactiveCommands;
    }

    public boolean isEnableScheduling() {
        return enableScheduling;
    }

    public long getSchedulingInterval() {
        return schedulingInterval;
    }

    public long getRefreshInterval() {
        return refreshInterval;
    }
}
