package com.github.priytam;

import static java.util.Objects.requireNonNull;

import com.github.priytam.event.Event;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.Value;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class Subscription  implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Subscription.class);

    private final Map<Class<? extends Event>, HandlerAndSubscription<? extends Event>> subscriptions = new ConcurrentHashMap<>();
    private final DelayQueue delayQueue;
    private final QueueSettings queueSettings;


    public Subscription(DelayQueue delayQueue, QueueSettings queueSettings) {
        this.delayQueue = delayQueue;
        this.queueSettings = queueSettings;
    }

    void refreshSubscriptions() {
        subscriptions.replaceAll((k, v) -> {
                    v.subscription.dispose();
                    return createNewSubscription(v);
                }
        );
    }

    public <T extends Event> boolean unsubscribe(@NotNull Class<T> eventType) {
        requireNonNull(eventType, "event type");
        HandlerAndSubscription<? extends Event> subscription = subscriptions.remove(eventType);
        if (subscription != null) {
            subscription.subscription.dispose();
            return true;
        }
        return false;
    }

    public <T extends Event> void subscribe(@NotNull Class<T> eventType,
                                            @NotNull Function<@NotNull T, @NotNull Mono<Boolean>> handler,
                                            int parallelism) {
        requireNonNull(eventType, "event type");
        requireNonNull(handler, "handler");
        checkInRange(parallelism, 1, 100, "parallelism");

        subscriptions.computeIfAbsent(eventType, re -> {
            InnerSubscriber<T> subscription = createSubscription(eventType, handler, parallelism);
            queueSettings.getMetrics().registerReadyToProcessSupplier(eventType,
                    () -> queueSettings.getMetricsCommands().llen(toQueueName(eventType)));
            return new HandlerAndSubscription<>(eventType, handler, parallelism, subscription);
        });
    }

    private <T extends Event> HandlerAndSubscription<T> createNewSubscription(HandlerAndSubscription<T> old) {
        LOG.info("refreshing subscription for [{}]", old.type.getName());
        return new HandlerAndSubscription<>(
                old.type,
                old.handler,
                old.parallelism,
                createSubscription(old.type, old.handler, old.parallelism)
        );
    }

    private <T extends Event> InnerSubscriber<T> createSubscription(Class<T> eventType,
                                                                    Function<T, Mono<Boolean>> handler,
                                                                    int parallelism) {
        StatefulRedisConnection<String, String> pollingConnection = queueSettings.getClient().connect();
        InnerSubscriber<T> subscription =
                new InnerSubscriber<>(queueSettings.getContextHandler(), handler, parallelism,
                        pollingConnection, queueSettings.getHandlerScheduler(), delayQueue::dequeue);
        String queue = toQueueName(eventType);

        // todo reconnect instead of reset + flux concat instead of generate sink.next(0)
        Flux
                .generate(sink -> sink.next(0))
                .flatMap(
                        r -> pollingConnection
                                .reactive()
                                .brpop(queueSettings.getPollingTimeout().toMillis() / 1000, queue)
                                .doOnError(e -> {
                                    if (e instanceof RedisCommandTimeoutException) {
                                        LOG.debug("polling command timed out ({} seconds)",
                                                queueSettings.getPollingTimeout().toMillis() / 1000);
                                    } else {
                                        LOG.warn("error polling redis queue", e);
                                    }
                                    pollingConnection.reset();
                                })
                                .onErrorReturn(KeyValue.empty(eventType.getName())),
                        1, // it doesn't make sense to do requests on single connection in parallel
                        parallelism
                )
                .publishOn(queueSettings.getHandlerScheduler(), parallelism)
                .defaultIfEmpty(KeyValue.empty(queue))
                .filter(Value::hasValue)
                .doOnNext(v -> queueSettings.getMetrics().incrementDequeueCounter(eventType))
                .map(Value::getValue)
                .map(v -> queueSettings.getSerializerDeserializer().deserialize(eventType, v))
                .onErrorContinue((e, r) -> LOG.warn("Unable to deserialize [{}]", r, e))
                .subscribe(subscription);

        return subscription;
    }


    private String toQueueName(Class<? extends Event> cls) {
        return queueSettings.getDataSetPrefix() + cls.getSimpleName().toLowerCase();
    }

    private static int checkInRange(int value, int min, int max, String message) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(String.format("'%s' should be inside range [%s, %s]", message, min, max));
        }
        return value;
    }

    @Override
    public void close() throws IOException {
        subscriptions.forEach((k, v) -> v.subscription.dispose());
        queueSettings.getMetricsCommands().getStatefulConnection().close();
        queueSettings.getHandlerScheduler().dispose();
    }

    private static class HandlerAndSubscription<T extends Event> {
        private final Class<T> type;
        private final Function<T, Mono<Boolean>> handler;
        private final int parallelism;
        private final Disposable subscription;

        private HandlerAndSubscription(
                Class<T> type,
                Function<T, Mono<Boolean>> handler,
                int parallelism,
                Disposable subscription
        ) {
            this.type = type;
            this.handler = handler;
            this.parallelism = parallelism;
            this.subscription = subscription;
        }
    }
}
