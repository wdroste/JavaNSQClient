package com.github.brainlag.nsq;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.collect.Maps;

import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class NSQConsumer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(NSQConsumer.class);

    private final NSQLookup lookup;
    private final String topic;
    private final String channel;
    private final NSQMessageCallback callback;
    private final NSQErrorCallback errorCallback;
    private final NSQConfig config;
    private boolean started = false;
    private int messagesPerBatch = 200;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<ServerAddress, Connection> connections = Maps.newHashMap();

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback) {
        this(lookup, topic, channel, callback, new NSQConfig());
    }

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback,
                       final NSQConfig config) {
        this(lookup, topic, channel, callback, config, null);
    }

    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final NSQMessageCallback callback,
                       final NSQConfig config, final NSQErrorCallback errCallback) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config;
        this.callback = callback;
        this.errorCallback = errCallback;

    }

    public NSQConsumer start() {
        if (!started) {
            started = true;
            scheduler.scheduleAtFixedRate(this::connect, 0, this.lookupPeriod, MILLISECONDS);
        }
        return this;
    }

    Connection createConnection(final ServerAddress serverAddress) {
        try {
            final Connection connection = new Connection(serverAddress, config);

            connection.setMessageCallback(this.callback);
            connection.setErrorCallback(this.errorCallback);
            connection.command(NSQCommand.instance("SUB " + this.topic + " " + this.channel));
            connection.command(NSQCommand.instance("RDY " + this.messagesPerBatch));

            return connection;
        } catch (final NoConnectionsException e) {
            LOG.warn("Could not create connection to server {} - {}", serverAddress, e.getMessage());
            return null;
        }
    }


    public void shutdown() {
        this.scheduler.shutdown();
        cleanClose();
    }

    private void cleanClose() {
        final NSQCommand command = NSQCommand.instance("CLS");
        try {
            for (final Connection connection : connections.values()) {
                final NSQFrame frame = connection.commandAndWait(command);
                if (frame != null && frame instanceof ErrorFrame) {
                    final String err = ((ErrorFrame) frame).getErrorMessage();
                    if (err.startsWith("E_INVALID")) {
                        throw new IllegalStateException(err);
                    }
                }
            }
        } catch (final TimeoutException e) {
            LOG.warn("No clean disconnect", e);
        }
    }

    private void connect() {
        Set<ServerAddress> removalSet = this.connections.values().stream()
                .filter(c -> !c.isConnected())
                .map(c -> c.getServerAddress())
                .collect(Collectors.toSet());
        removalSet.forEach(this.connections::remove);

        final Set<ServerAddress> newAddresses = lookupAddresses();
        final Set<ServerAddress> oldAddresses = this.connections.keySet();

        LOG.debug("Addresses NSQ connected to: {}", newAddresses);
        if (newAddresses.isEmpty()) {
            LOG.warn("No NSQLookup server connections or topic does not exist.");
        } else {
            for (final ServerAddress server : difference(oldAddresses, newAddresses)) {
                LOG.info("Remove connection {}", server.toString());
                this.connections.get(server).close();
                this.connections.remove(server);
            }

            difference(newAddresses, oldAddresses)
                    .stream()
                    .filter(server -> !this.connections.containsKey(server))
                    .map(this::createConnection)
                    .filter(conn -> null != conn)
                    .forEach(conn -> this.connections.put(conn.getServerAddress(), conn));
        }
    }

    private Set<ServerAddress> lookupAddresses() {
        return this.lookup.lookup(this.topic);
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }
}
