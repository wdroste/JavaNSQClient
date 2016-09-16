package com.github.brainlag.nsq;

import java.net.InetAddress;
import java.net.UnknownHostException;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

@Data
public class NSQConfig {
    private static final Logger LOG = LoggerFactory.getLogger(NSQConfig.class);

    private String clientId;
    private String hostname;
    private boolean featureNegotiation = true;
    private Integer heartbeatInterval = null;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeout = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = null;
    private String userAgent = null;
    private Integer msgTimeout = null;
    private SslContext sslContext = null;
    private EventLoopGroup eventLoopGroup = null;
    private int messagesPerBatch = 200;

    public NSQConfig() {
        try {
            clientId = InetAddress.getLocalHost().getHostName();
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
            userAgent = "JavaNSQClient";
        } catch (UnknownHostException e) {
            LOG.error("Local host name could not resolved", e);
        }
    }


    public void setSslContext(SslContext sslContext) {
        Preconditions.checkNotNull(sslContext);
        tlsV1 = true;
        this.sslContext = sslContext;
    }

    public enum Compression {NO_COMPRESSION, DEFLATE, SNAPPY}
}
