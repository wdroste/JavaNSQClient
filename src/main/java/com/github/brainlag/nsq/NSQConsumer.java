package com.github.brainlag.nsq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.lookup.NSQLookup;

public class NSQConsumer extends AbstractNSQConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(NSQConsumer.class);

    private final NSQMessageCallback callback;
    private final NSQErrorCallback errorCallback;

    public NSQConsumer(final NSQLookup lookup,
                       final String topic,
                       final String channel,
                       final NSQMessageCallback callback,
                       final NSQConfig config,
                       final NSQErrorCallback errCallback) {
        super(lookup, topic, channel, config);
        this.callback = callback;
        this.errorCallback = errCallback;
    }

    public NSQConsumer start() {
        startLookupSchedule();
        return this;
    }

    protected NSQConnection createConnection(final ServerAddress serverAddress) {
        try {
            final NSQConnection connection = new NSQConnection(
                    serverAddress, config, this.callback, this.errorCallback);
            connection.command(NSQCommand.instance("SUB " + this.topic + " " + this.channel));
            connection.command(NSQCommand.instance("RDY " + this.config.getMessagesPerBatch()));

            return connection;
        } catch (final NoConnectionsException e) {
            LOG.warn("Could not create connection to server {} - {}", serverAddress, e.getMessage());
            return null;
        }
    }
}
