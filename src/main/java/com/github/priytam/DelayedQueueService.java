package com.github.priytam;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.priytam.context.EventContextHandler;
import com.github.priytam.context.NoopEventContextHandler;
import com.github.priytam.event.Event;
import com.github.priytam.metrics.Metrics;
import com.github.priytam.metrics.NoopMetrics;
import io.lettuce.core.Limit;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class DelayedQueueService implements Closeable {
    private static final String DELIMITER = "###";
    private final DelayQueue delayQueue;
    private final Subscription subscription;
    private final Dispatcher dispatcher;

    public DelayedQueueService(Builder builder) {
        ObjectMapper mapper = requireNonNull(builder.mapper, "object mapper");
        RedisClient client = requireNonNull(builder.client, "redis client");
        EventContextHandler contextHandler = requireNonNull(builder.eventContextHandler, "event context handler");
        Duration pollingTimeout = checkNotShorter(builder.pollingTimeout, Duration.ofMillis(50), "polling interval");
        Duration lockTimeout = Duration.ofSeconds(2);
        int retryAttempts = checkInRange(builder.retryAttempts, 1, 100, "retry attempts");
        Scheduler handlerScheduler = requireNonNull(builder.scheduler, "scheduler");
        Metrics metrics = requireNonNull(builder.metrics, "metrics");
        String dataSetPrefix = requireNonNull(builder.dataSetPrefix, "data set prefix");
        Limit schedulingBatchSize = Limit.from(checkInRange(builder.schedulingBatchSize, 1, 1000, "scheduling batch size"));

        String zsetName = dataSetPrefix + "delayed_events";
        String lockKey = dataSetPrefix + "delayed_events_lock";
        String metadataHset = dataSetPrefix + "events";

        RedisCommands<String, String> dispatchCommands = client.connect().sync();
        RedisCommands<String, String> metricsCommands = client.connect().sync();
        RedisReactiveCommands<String, String> reactiveCommands = client.connect().reactive();

        long schedulingInterval = checkNotShorter(builder.schedulingInterval, Duration.ofMillis(50), "scheduling interval").toNanos();

        long refreshInterval = 0;
        if (builder.refreshSubscriptionInterval != null) {
            refreshInterval = checkNotShorter(
                    builder.refreshSubscriptionInterval,
                    Duration.ofMinutes(5),
                    "refresh subscription interval"
            ).toNanos();
        }

        SerializerDeserializer serializerDeserializer = new SerializerDeserializer(mapper);
        QueueSettings queueSettings =
                new QueueSettings(builder.enableScheduling,
                        schedulingInterval, refreshInterval,
                        serializerDeserializer, client, contextHandler,
                        pollingTimeout, lockTimeout, retryAttempts, handlerScheduler,
                        metrics, dataSetPrefix, schedulingBatchSize, zsetName, lockKey,
                        metadataHset, reactiveCommands, dispatchCommands, metricsCommands);
        delayQueue = new DelayQueue(queueSettings);
        subscription = new Subscription(delayQueue, queueSettings);
        dispatcher = new Dispatcher(subscription, queueSettings);
    }


    public Mono<Void> enqueue(@NotNull Event event, @NotNull Duration delay) {
        return delayQueue.enqueueNonBlocking(event, delay);
    }

    public Mono<Boolean> dequeue(Event event) {
        return delayQueue.dequeue(event).map(r -> true);
    }

    public <T extends Event> void subscribe(@NotNull Class<T> eventType,
                                            @NotNull Function<@NotNull T, @NotNull Mono<Boolean>> handler,
                                            int parallelism) {
        subscription.subscribe(eventType, handler, parallelism);
    }

    public <T extends Event> boolean unsubscribe(@NotNull Class<T> eventType) {
        return subscription.unsubscribe(eventType);
    }

    public void dispatch() {
        dispatcher.dispatch();
    }

    private static int checkInRange(int value, int min, int max, String message) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(String.format("'%s' should be inside range [%s, %s]", message, min, max));
        }
        return value;
    }

    private static Duration checkNotShorter(Duration duration, Duration ref, String msg) {
        requireNonNull(duration, "given duration should be not null");

        if (duration.compareTo(ref) < 0) {
            throw new IllegalArgumentException(String.format("%s with value %s should be not shorter than %s", msg, duration, ref));
        }
        return duration;
    }

    public static String getKey(Event event) {
        return event.getClass().getName() + DELIMITER + event.getId();
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        dispatcher.close();
        subscription.close();
        delayQueue.close();
    }

    @NotNull
    public static Builder delayedQueueService() {
        return new Builder();
    }


    public static final class Builder {
        private RedisClient client;
        private ObjectMapper mapper = new ObjectMapper();
        private Duration pollingTimeout = Duration.ofSeconds(1);
        private boolean enableScheduling = true;
        private Duration schedulingInterval = Duration.ofMillis(500);
        private int schedulingBatchSize = 100;
        private Scheduler scheduler = Schedulers.elastic();
        private int retryAttempts = 70;
        private Metrics metrics = new NoopMetrics();
        private EventContextHandler eventContextHandler = new NoopEventContextHandler();
        private String dataSetPrefix = "de_";
        private Duration refreshSubscriptionInterval;

        private Builder() {
        }

        @NotNull
        public Builder mapper(@NotNull ObjectMapper val) {
            mapper = val;
            return this;
        }

        @NotNull
        public Builder client(@NotNull RedisClient val) {
            client = val;
            return this;
        }

        @NotNull
        public Builder pollingTimeout(@NotNull Duration val) {
            pollingTimeout = val;
            return this;
        }

        @NotNull
        public Builder enableScheduling(boolean val) {
            enableScheduling = val;
            return this;
        }

        @NotNull
        public Builder schedulingInterval(@NotNull Duration val) {
            schedulingInterval = val;
            return this;
        }

        @NotNull
        public Builder retryAttempts(int val) {
            retryAttempts = val;
            return this;
        }

        @NotNull
        public Builder handlerScheduler(@NotNull Scheduler val) {
            scheduler = val;
            return this;
        }

        @NotNull
        public Builder metrics(@NotNull Metrics val) {
            metrics = val;
            return this;
        }

        @NotNull
        public Builder eventContextHandler(@NotNull EventContextHandler val) {
            eventContextHandler = val;
            return this;
        }

        @NotNull
        public Builder dataSetPrefix(@NotNull String val) {
            dataSetPrefix = val;
            return this;
        }

        @NotNull
        public Builder schedulingBatchSize(int val) {
            schedulingBatchSize = val;
            return this;
        }

        @NotNull
        public Builder refreshSubscriptionsInterval(@NotNull Duration val) {
            refreshSubscriptionInterval = val;
            return this;
        }

        @NotNull
        public DelayedQueueService build() {
            return new DelayedQueueService(this);
        }

    }

}
