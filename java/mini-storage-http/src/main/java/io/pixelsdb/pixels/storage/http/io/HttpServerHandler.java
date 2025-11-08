package io.pixelsdb.pixels.storage.http.io;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpUtil;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@ChannelHandler.Sharable
public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject>
{
    protected Runnable serverCloser;

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    // demo handler
    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception
    {
        final byte[] payload = {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'};

        if (msg instanceof HttpRequest)
        {
            HttpRequest req = (HttpRequest) msg;

            boolean keepAlive = HttpUtil.isKeepAlive(req);
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(payload));
            response.headers()
                    .set(CONTENT_TYPE, "text/plain")
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());

            if (keepAlive)
            {
                if (!req.protocolVersion().isKeepAliveDefault())
                {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            }
            else
            {
                response.headers().set(CONNECTION, CLOSE);
            }

            ChannelFuture f = ctx.write(response);

            if (!keepAlive)
            {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    // By default, response with 500 Internal Server Error and close the connection on exception.
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, INTERNAL_SERVER_ERROR);
        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        ChannelFuture f = ctx.writeAndFlush(response);
        f.addListener(ChannelFutureListener.CLOSE);
        ctx.close();
    }

    public void setServerCloser(Runnable serverCloser) {
        this.serverCloser = serverCloser;
    }
}
