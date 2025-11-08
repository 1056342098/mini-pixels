/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.storage.http.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.concurrent.*;

public class HttpInputStream extends InputStream
{
    private static final Logger logger = LogManager.getLogger(HttpInputStream.class);

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * The temporary buffer used for storing the chunks.
     */
    private final BlockingQueue<ByteBuf> contentQueue;

    /**
     * The http server for receiving input stream.
     */
    private final HttpServer httpServer;

    /**
     * The thread to run http server.
     */
    private final ExecutorService executorService;

    /**
     * The future of http server.
     */
    private final CompletableFuture<Void> httpServerFuture;

    public HttpInputStream(String host, int port) throws CertificateException, SSLException
    {
        this.open = true;
        this.contentQueue = new LinkedBlockingDeque<>();

        /*
         * 修改点: 这里的Handler仍然需要是HttpServer可以接受的类型。
         * 我们创建一个新的Handler，它会接收被HttpServer聚合后的FullHttpRequest，
         * 然后从中提取出ByteBuf放入队列。这虽然不是最高效的流式处理，
         * 但是在不改变HttpServer的前提下的最佳实践。
         */
        this.httpServer = new HttpServer(new AggregatedHttpServerHandler(this.contentQueue));

        this.executorService = Executors.newSingleThreadExecutor(); // 使用 newSingleThreadExecutor 更符合单任务场景
        this.httpServerFuture = CompletableFuture.runAsync(() -> {
            try
            {
                /*
                 * 修改点: 遵从HttpServer的API，只传递port。
                 * 这意味着服务器将监听在 0.0.0.0 (所有网络接口) 上。
                 */
                this.httpServer.serve(port);
            }
            catch (InterruptedException e)
            {
                logger.error("http server interrupted", e);
                // 恢复中断状态
                Thread.currentThread().interrupt();
            }
        }, this.executorService);
    }

    @Override
    public int read() throws IOException
    {
        assertOpen();
        if (emptyData())
        {
            return -1;
        }

        ByteBuf content = this.contentQueue.peek();
        int b = -1;
        if (content != null)
        {
            b = content.readUnsignedByte();
            if (!content.isReadable())
            {
                // 消费完后，从队列中移除并释放
                this.contentQueue.poll();
                content.release();
            }
        }
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException
    {
        assertOpen();

        ByteBuf content;
        int readBytes = 0;
        while (readBytes < len)
        {
            if (emptyData())
            {
                return readBytes > 0 ? readBytes : -1;
            }
            content = this.contentQueue.peek();
            if (content == null)
            {
                return readBytes > 0 ? readBytes : -1;
            }

            try
            {
                int readLen = Math.min(len - readBytes, content.readableBytes());
                content.readBytes(buf, off + readBytes, readLen);
                readBytes += readLen;
                if (!content.isReadable())
                {
                    contentQueue.poll();
                    content.release();
                }
            } catch (Exception e) {
                if (content != null && !content.isReadable())
                {
                    contentQueue.poll();
                    content.release();
                }
                throw new IOException("Error reading from buffer", e);
            }
        }

        return readBytes;
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            this.httpServerFuture.complete(null); 
            this.httpServer.close();

            /*
             * 修改点 (关键): 正确关闭线程池以防止线程泄露。
             */
            this.executorService.shutdown();
            try {
                if (!this.executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    this.executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }


            /*
             * 修改点 (关键): 清理队列中所有残留的ByteBuf，防止内存泄露。
             */
            ByteBuf buf;
            while ((buf = contentQueue.poll()) != null) {
                buf.release();
            }
        }
    }

    private boolean emptyData() throws IOException
    {
        int tries = 0;
        // 修改点: 直接使用常量，保持一致性
        while (tries < Constants.MAX_STREAM_RETRY_COUNT && this.contentQueue.isEmpty() && !this.httpServerFuture.isDone())
        {
            try
            {
                tries++;
                Thread.sleep(Constants.STREAM_DELAY_MS);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for data", e);
            }
        }
        if (tries == Constants.MAX_STREAM_RETRY_COUNT && this.contentQueue.isEmpty())
        {
            logger.error("HttpInputStream failed to receive data in time. Retried {} times.", tries);
        }

        return this.contentQueue.isEmpty();
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            // 修改点: 提供更明确的异常信息
            throw new IllegalStateException("http input stream is closed");
        }
    }

    /**
     * 修改点: 这是一个新的Handler，用于处理被HttpServer聚合后的FullHttpRequest。
     * 它继承自HttpServerHandler以满足HttpServer构造函数的要求。
     * 它的作用是从完整的请求中提取出内容(ByteBuf)，并放入我们的队列中。
     */
    public static class AggregatedHttpServerHandler extends HttpServerHandler // 假设您项目中的基类是 HttpServerHandler
    {
        private final BlockingQueue<ByteBuf> contentQueue;

        public AggregatedHttpServerHandler(BlockingQueue<ByteBuf> contentQueue)
        {
            this.contentQueue = contentQueue;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
        {
            // 因为HttpServer内部有聚合器，所以这里收到的必然是FullHttpRequest
            if (msg instanceof FullHttpRequest)
            {
                FullHttpRequest req = (FullHttpRequest) msg;
                if (req.method() == HttpMethod.POST)
                {
                    ByteBuf content = req.content();
                    if (content.isReadable())
                    {
                        // 增加引用计数，因为我们将异步消费它
                        content.retain();
                        this.contentQueue.add(content);
                    }
                }
                // 无论请求是否有效，都应该给客户端一个响应
                sendResponse(ctx, req, HttpResponseStatus.OK);
            }
        }

        private void sendResponse(ChannelHandlerContext ctx, HttpRequest req, HttpResponseStatus status)
        {
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

            if (!HttpUtil.isKeepAlive(req)) {
                ctx.writeAndFlush(response).addListener(future -> ctx.close());
            } else {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                ctx.writeAndFlush(response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception caught in http server handler", cause);
            ctx.close();
        }
    }
}