package com.github.brainlag.nsq;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.MessageFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.github.brainlag.nsq.netty.NSQClientInitializer;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Connection {
    private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<Connection> STATE = AttributeKey.valueOf("Connection.state");
    private static EventLoopGroup defaultGroup = null;
    private final ServerAddress address;
    private final Channel channel;
    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);
    private final EventLoopGroup eventLoopGroup;
    private final NSQConfig config;
    private NSQMessageCallback messageCallback = null;
    private NSQErrorCallback errorCallback = null;

    private int messagesPerBatch = 200;
    private final AtomicLong totalMessages = new AtomicLong(0l);

    public Connection(final ServerAddress serverAddress, final NSQConfig config) throws NoConnectionsException {
        this.address = serverAddress;
        this.config = config;
        val bootstrap = new Bootstrap();
        this.eventLoopGroup = config.getEventLoopGroup() != null ? config.getEventLoopGroup() : getDefaultGroup();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        // Start the connection attempt.
        final ChannelFuture future = bootstrap.connect(
                new InetSocketAddress(serverAddress.getHost(), serverAddress.getPort()));

        // Wait until the connection attempt succeeds or fails.
        this.channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        LOG.info("Created connection: {}", serverAddress);
        this.channel.attr(STATE).set(this);
        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        this.channel.write(buf);
        this.channel.flush();

        //identify
        val ident = NSQCommand.instance("IDENTIFY", config.toString().getBytes());
        try {
            final NSQFrame response = this.commandAndWait(ident);
            if (response != null) {
                LOG.info("Server identification: {}", ((ResponseFrame) response).getMessage());
            }
        } catch (final TimeoutException e) {
            LOG.error("Creating connection timed out", e);
            close();
        }
    }

    private EventLoopGroup getDefaultGroup() {
        if (defaultGroup == null) {
            defaultGroup = new NioEventLoopGroup();
        }
        return defaultGroup;
    }

    public boolean isConnected() {
        return channel.isActive();
    }

    public boolean isRequestInProgress() {
        return requests.size() > 0;
    }

    public void incoming(final NSQFrame frame) {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
                heartbeat();
                return;
            } else {
                if (!requests.isEmpty()) {
                    try {
                        responses.offer(frame, 20, SECONDS);
                    } catch (final InterruptedException e) {
                        LOG.error("Thread was interrupted, probably shutting down", e);
                        close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            if (this.errorCallback != null) {
                this.errorCallback.error(NSQException.of((ErrorFrame) frame));
            }
            this.responses.add(frame);
            return;
        }

        if (frame instanceof MessageFrame) {
            val msg = (MessageFrame) frame;
            val message = NSQMessage.builder()
                    .connection(this)
                    .id(msg.getMessageId())
                    .message(msg.getMessageBody())
                    .timestamp(new Date(NANOSECONDS.toMillis(msg.getTimestamp())))
                    .build();
            this.processMessage(message);
            return;
        }
        LOG.warn("Unknown frame type: " + frame);
    }

    void processMessage(final NSQMessage message) {
        this.messageCallback.message(message);

        final long tot = totalMessages.incrementAndGet();
        if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
            //request some more!
            message.rdy(messagesPerBatch);
        }
    }

    private void heartbeat() {
        LOG.trace("HEARTBEAT!");
        command(NSQCommand.instance("NOP"));
    }

    public void setErrorCallback(final NSQErrorCallback callback) {
        errorCallback = callback;
    }

    public void close() {
        LOG.info("Closing  connection: " + this);
        channel.disconnect();
    }

    public NSQFrame commandAndWait(final NSQCommand command) throws TimeoutException {
        try {
            if (!requests.offer(command, 15, SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            this.responses.clear(); //clear the response queue if needed.
            final ChannelFuture fut = command(command);

            if (!fut.await(15, SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            final NSQFrame frame = this.responses.poll(15, SECONDS);
            if (frame == null) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            this.requests.poll(); //clear the request object
            return frame;
        } catch (final InterruptedException e) {
            close();
            LOG.warn("Thread was interrupted!", e);
        }
        return null;
    }

    public ChannelFuture command(final NSQCommand command) {
        return this.channel.writeAndFlush(command);
    }

    public ServerAddress getServerAddress() {
        return address;
    }

    public NSQConfig getConfig() {
        return config;
    }

    public void setMessageCallback(final NSQMessageCallback messageCallback) {
        this.messageCallback = messageCallback;
    }
}
