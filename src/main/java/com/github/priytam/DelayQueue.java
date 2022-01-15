package com.github.priytam;

import static com.github.priytam.DelayedQueueService.getKey;
import static io.lettuce.core.ZAddArgs.Builder.nx;
import static java.util.Objects.requireNonNull;

import com.github.priytam.event.Event;
import com.github.priytam.event.EventEnvelope;
import io.lettuce.core.TransactionResult;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class DelayQueue implements Closeable {

    private final Scheduler single = Schedulers.newSingle("redis-single");
    private final QueueSettings queueSettings;


    public DelayQueue(QueueSettings queueMetaData) {
        this.queueSettings = queueMetaData;
    }


    public Mono<Void> enqueueNonBlocking(@NotNull Event event, @NotNull Duration delay) {
        requireNonNull(event, "event");
        requireNonNull(delay, "delay");
        requireNonNull(event.getId(), "event id");

        return Mono.subscriberContext()
                .flatMap(ctx -> enqueueWithDelayInner(event, delay, queueSettings.getContextHandler().eventContext(ctx)))
                .then();
    }

    public Mono<TransactionResult> dequeue(Event event) {
        return executeInTransaction(() -> {
            String key = getKey(event);
            queueSettings.getReactiveCommands().hdel(queueSettings.getMetadataHset(), key).subscribeOn(single).subscribe();
            queueSettings.getReactiveCommands().zrem(queueSettings.getZsetName(), key).subscribeOn(single).subscribe();
        });
    }

    private Mono<TransactionResult> enqueueWithDelayInner(Event event, Duration delay, Map<String, String> context) {
        return executeInTransaction(() -> {
            String key = getKey(event);
            String rawEnvelope = queueSettings.getSerializerDeserializer().serialize(EventEnvelope.create(event, context));

            queueSettings.getReactiveCommands().hset(queueSettings.getMetadataHset(),
                    key, rawEnvelope).subscribeOn(single).subscribe();
            queueSettings.getReactiveCommands().zadd(queueSettings.getZsetName(), nx(),
                    (System.currentTimeMillis() + delay.toMillis()), key).subscribeOn(single).subscribe();
        }).doOnNext(v -> queueSettings.getMetrics().incrementEnqueueCounter(event.getClass()));
    }

    private Mono<TransactionResult> executeInTransaction(Runnable commands) {
        return Mono.defer(() -> {
            queueSettings.getReactiveCommands().multi().subscribeOn(single).subscribe();
            commands.run();
            // todo reconnect instead of reset
            return queueSettings.getReactiveCommands()
                    .exec()
                    .subscribeOn(single)
                    .doOnError(e -> queueSettings.getReactiveCommands().getStatefulConnection().reset());
        }).subscribeOn(single);
    }

    @Override
    public void close() throws IOException {
        single.dispose();
        queueSettings.getReactiveCommands().getStatefulConnection().close();
    }
}
