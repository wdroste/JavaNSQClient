package com.github.brainlag.nsq.rx;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Builder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.brainlag.nsq.Connection;
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.collect.Maps;

/**
 * Reactive Streams based consumer with backpressure.
 */
@Builder
public class RxNSQConsumer implements Publisher<NSQMessage> {
    final NSQLookup lookup;
    final String topic;
    final String channel;
    final NSQConfig config;


    private final Map<ServerAddress, Connection> connections = Maps.newHashMap();

    @Override
    public void subscribe(Subscriber<? super NSQMessage> subscriber) {
        //this.scheduler.scheduleAtFixedRate(this::connect, 0, this.lookupPeriod, MILLISECONDS);

        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long l) {

            }

            @Override
            public void cancel() {

            }
        });

    }


    private final AtomicLong totalMessages = new AtomicLong(0L);
    private volatile long nextTimeout = 0;
    private boolean started = false;
    private int messagesPerBatch = 200;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private Executor executor = Executors.newCachedThreadPool();
    private Optional<ScheduledFuture<?>> timeout = Optional.empty();


//    public NSQConsumer start() {
//        if (!started) {
//            started = true;
//        }
//        return this;
//    }
//
//    Connection createConnection(final ServerAddress serverAddress) {
//        try {
//            final Connection connection = new Connection(serverAddress, config);
//
//            connection.setMessageCallback(this);
//            connection.setErrorCallback(this.errorCallback);
//            connection.command(NSQCommand.instance("SUB " + this.topic + " " + this.channel));
//            connection.command(NSQCommand.instance("RDY " + this.messagesPerBatch));
//
//            return connection;
//        } catch (final NoConnectionsException e) {
//            LOG.warn("Could not create connection to server {} - {}", serverAddress, e.getMessage());
//            return null;
//        }
//    }
//
//    void processMessage(final NSQMessage message) {
//        if (callback == null) {
//            LOG.warn("NO Callback, dropping message: {}", message);
//        } else {
//            try {
//                executor.execute(() -> callback.message(message));
//                if (this.nextTimeout > 0) {
//                    updateTimeout(message, -500);
//                }
//            } catch (RejectedExecutionException re) {
//                LOG.trace("Backing off");
//                message.requeue();
//                updateTimeout(message, 500);
//            }
//        }
//
//        final long tot = totalMessages.incrementAndGet();
//        if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
//            //request some more!
//            rdy(message, messagesPerBatch);
//        }
//    }
//
//    private void updateTimeout(final NSQMessage message, long change) {
//        rdy(message, 0);
//        LOG.trace("RDY 0! Halt Flow.");
//        if (timeout.isPresent()) {
//            timeout.get().cancel(true);
//        }
//        Date newTimeout = calculateTimeoutDate(change);
//        if (newTimeout != null) {
//            timeout = Optional.of(scheduler.schedule(() -> {
//                rdy(message, 1); // test the waters
//            }, 0, MILLISECONDS));
//        }
//    }
//
//    private void rdy(final NSQMessage message, int size) {
//        message.getConnection().command(NSQCommand.instance("RDY " + size));
//    }
//
//    private Date calculateTimeoutDate(final long i) {
//        if (System.currentTimeMillis() - nextTimeout + i > 50) {
//            nextTimeout += i;
//            return new Date();
//        } else {
//            nextTimeout = 0;
//            return null;
//        }
//    }
//
//    public void shutdown() {
//        scheduler.shutdown();
//        cleanClose();
//    }
//
//    private void cleanClose() {
//        final NSQCommand command = NSQCommand.instance("CLS");
//        try {
//            for (final Connection connection : connections.values()) {
//                final NSQFrame frame = connection.commandAndWait(command);
//                if (frame != null && frame instanceof ErrorFrame) {
//                    final String err = ((ErrorFrame) frame).getErrorMessage();
//                    if (err.startsWith("E_INVALID")) {
//                        throw new IllegalStateException(err);
//                    }
//                }
//            }
//        } catch (final TimeoutException e) {
//            LOG.warn("No clean disconnect", e);
//        }
//    }
//
//    private void connect() {
//        for (final Iterator<Map.Entry<ServerAddress, Connection>> it = connections.entrySet().iterator(); it.hasNext(); ) {
//            if (!it.next().getValue().isConnected()) {
//                it.remove();
//            }
//        }
//
//        final Set<ServerAddress> newAddresses = lookupAddresses();
//        final Set<ServerAddress> oldAddresses = connections.keySet();
//
//        LOG.debug("Addresses NSQ connected to: {}", newAddresses);
//        if (newAddresses.isEmpty()) {
//            // in case the lookup server is not reachable for a short time we don't we dont want to
//            // force close connection
//            // just log a message and keep moving
//            LOG.warn("No NSQLookup server connections or topic does not exist.");
//        } else {
//            for (final ServerAddress server : difference(oldAddresses, newAddresses)) {
//                LOG.info("Remove connection {}", server.toString());
//                this.connections.get(server).close();
//                this.connections.remove(server);
//            }
//
//            difference(newAddresses, oldAddresses)
//                    .stream()
//                    .filter(server -> !this.connections.containsKey(server))
//                    .map(this::createConnection)
//                    .filter(conn -> null != conn)
//                    .forEach(conn -> connections.put(conn.getServerAddress(), conn));
//        }
//    }
//
//    /**
//     * This is the executor where the callbacks happen.
//     * The executer can only changed before the client is started.
//     * Default is a cached threadpool.
//     */
//    public NSQConsumer setExecutor(final Executor executor) {
//        if (!started) {
//            this.executor = executor;
//        }
//        return this;
//    }
//
//    private Set<ServerAddress> lookupAddresses() {
//        return lookup.lookup(topic);
//    }
//
//    @Override
//    public void close() throws IOException {
//        shutdown();
//    }

}
