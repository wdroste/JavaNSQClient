package com.github.brainlag.nsq.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.brainlag.nsq.Connection;
import com.github.brainlag.nsq.frames.NSQFrame;


public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {
    private static final Logger LOG = LoggerFactory.getLogger(NSQHandler.class);

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Connection connection = ctx.channel().attr(Connection.STATE).get();
        if (connection != null) {
            LOG.info("Channel disconnected! {}", connection);
        } else {
            LOG.error("No connection set for {}", ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        LOG.error("NSQHandler exception caught", cause);

        ctx.channel().close();
        Connection con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            con.close();
        } else {
            LOG.warn("No connection set for {}", ctx.channel());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) throws Exception {
        final Connection con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            ctx.channel().eventLoop().execute(() -> con.incoming(msg));
        } else {
            LOG.warn("No connection set for ", ctx.channel());
        }
    }
}
