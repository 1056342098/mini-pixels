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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
// [新增导入]: 引入List、Future和并发安全的List实现
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future; // 确保导入 java.util.concurrent.Future

public class HttpOutputStream extends OutputStream
{
    private static final Logger logger = LogManager.getLogger(HttpOutputStream.class);

        /**
     * indicates whether the http is still open / valid
     */
    private boolean open;

    /**
     * The schema of http.
     * Default value is http.
     */
    private final String schema = "http";

    /**
     * The host of http.
     */
    private String host;

    /**
     * The port of http.
     */
    private int port;

    /**
     * The uri of http.
     */
    private String uri;

    /**
     * The maximum retry count.
     */
    private static final int MAX_RETRIES = Constants.MAX_STREAM_RETRY_COUNT;

    /**
     * The delay between two tries.
     */
    private static final long RETRY_DELAY_MS = Constants.STREAM_DELAY_MS;

    /**
     * The temporary buffer used for storing the chunks.
     */
    private final byte[] buffer;

    /**
     * The position in the buffer.
     */
    private int bufferPosition;

    /**
     * The capacity of buffer.
     */
    private int bufferCapacity;
    
    private final ExecutorService executorService;
    private final AsyncHttpClient httpClient;
    private final BlockingQueue<byte[]> contentQueue;

    // ========================================================================
    // [修改]: 添加一个线程安全的列表来跟踪所有“正在发送”的请求
    // 我们使用CopyOnWriteArrayList，因为它对于“读多写少”的场景非常高效
    // （这里主要是添加和（在监听器中）移除）
    private final List<Future<Response>> outstandingSends = new CopyOnWriteArrayList<>();
    // ========================================================================

    public HttpOutputStream(String host, int port, int bufferCapacity) {
        this.open = true;
        this.host = host;
        this.port = port;
        this.uri = this.schema + "://" + host + ":" + port;
        this.bufferCapacity = bufferCapacity;
        this.buffer = new byte[bufferCapacity];
        this.bufferPosition = 0;

        // [修改 1]: 配置AsyncHttpClient以启用异步重试 (这部分是正确的)
        this.httpClient = Dsl.asyncHttpClient(Dsl.config()
                .setMaxRequestRetry(MAX_RETRIES) // 使用类中定义的常量
                .build());

        this.executorService = Executors.newSingleThreadExecutor();
        this.contentQueue = new LinkedBlockingQueue<>();

        // [修改 2]: 
        // 后台线程的逻辑保持不变，它仍然是消费队列并调用 sendContentAsync
        this.executorService.submit(() -> {
            while (true)
            {
                try
                {
                    byte[] content = contentQueue.take();
                    if (content.length == 0)
                    {
                        // 当收到 "0-byte" 信号...
                        // 1. 等待所有已提交的请求完成
                        // 2. 发送 "close" 信号
                        waitAndCloseStreamReader(); 
                        break;
                    }
                    sendContentAsync(content);
                } catch (InterruptedException e)
                {
                    logger.error("Background thread interrupted", e);
                    break;
                }
            }
            try
            {
                this.httpClient.close();
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Write an array to the S3 output http
     *
     * @param b
     * @throws IOException
     */
    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int offsetInBuf = off, remainToRead = len;
        int remainInBuffer;
        while (remainToRead > (remainInBuffer = this.buffer.length - bufferPosition))
        {
            System.arraycopy(buf, offsetInBuf, this.buffer, this.bufferPosition, remainInBuffer);
            this.bufferPosition += remainInBuffer;
            flushBufferAndRewind();
            offsetInBuf += remainInBuffer;
            remainToRead -= remainInBuffer;
        }
        System.arraycopy(buf, offsetInBuf, this.buffer, this.bufferPosition, remainToRead);
        this.bufferPosition += remainToRead;
    }

    @Override
    public void write(int b) throws IOException
    {
        this.assertOpen();
        if (this.bufferPosition >= this.buffer.length)
        {
            flushBufferAndRewind();
        }
        this.buffer[this.bufferPosition++] = (byte) b;
    }

    @Override
    public synchronized void flush()
    {
        assertOpen();
        if (this.bufferPosition > 0)
        {
            this.contentQueue.add(Arrays.copyOfRange(this.buffer, 0, this.bufferPosition));
            this.bufferPosition = 0;
        }
    }

    protected void flushBufferAndRewind() throws IOException
    {
        logger.debug("Sending {} bytes to http", this.bufferPosition);
        byte[] content = Arrays.copyOfRange(this.buffer, 0, this.bufferPosition);
        this.bufferPosition = 0;
        this.contentQueue.add(content);
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            this.open = false;
            if (this.bufferPosition > 0)
            {
                flush();
            }
            this.contentQueue.add(new byte[0]); 
            this.executorService.shutdown();
            try
            {
                if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS))
                {
                    this.executorService.shutdownNow();
                }
            } catch (InterruptedException e)
            {
                throw new IOException("Interrupted while waiting for termination", e);
            }
        }
    }

    // ========================================================================
    // [修改]: 修改 sendContentAsync 来跟踪 Future
    // [修改 3]
    private void sendContentAsync(byte[] content)
    {
        try
        {
            Request req = httpClient.preparePost(this.uri)
                    .setBody(ByteBuffer.wrap(content))
                    .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                    .addHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(content.length))
                    .addHeader(HttpHeaderNames.CONNECTION, "keep-alive")
                    .build();
            
            // 异步执行，不调用.get()
            ListenableFuture<Response> future = httpClient.executeRequest(req);
            
            // [新逻辑]: 将返回的 Future 添加到跟踪列表
            outstandingSends.add(future);
            
            // [新逻辑]: 添加一个监听器，当请求完成时（无论成功或失败），
            // 将其从列表中移除，避免内存泄漏。
            future.addListener(() -> outstandingSends.remove(future), null);

        } catch (Exception e)
        {
            // 这个catch现在只会捕捉到构建请求(preparePost)时发生的异常
            logger.error("Failed to prepare/send content", e);
        }
    }
    // ========================================================================


    /**
     * [修改]: 将 closeStreamReader 拆分为 "wait" 和 "close" 两步
     * 这个方法现在由后台线程在收到 "0-byte" 信号后调用。
     */
    private void waitAndCloseStreamReader()
    {
        // ========================================================================
        // [修改4 - 屏障]: 
        // 在发送关闭信号之前，等待所有已提交的异步请求完成。
        // 这创建了一个"屏障"(Barrier)，确保所有数据块在"Connection: close"之前被发送。
        try
        {
            // 拷贝一份列表快照，防止在迭代时列表被修改
            List<Future<Response>> futuresToWait = List.copyOf(outstandingSends);
            logger.debug("Waiting for {} outstanding sends to complete...", futuresToWait.size());
            
            for (Future<Response> f : futuresToWait) 
            {
                if (!f.isDone()) {
                   // .get() 会阻塞，直到这个特定的请求完成（或重试失败）
                   f.get();
                }
            }
            logger.debug("All outstanding sends completed.");
        } catch (Exception e) 
        {
            logger.error("Error waiting for outstanding sends to complete, proceeding with close anyway", e);
            // 即使有错误，我们仍然尝试关闭
        }
        // ========================================================================

        // [修改 5]: 发送真正的 "close" 信号
        // 屏障已经确保了所有数据都已发送，现在可以安全地关闭了。
        Request req = httpClient.preparePost(this.uri)
                .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                .addHeader(HttpHeaderNames.CONTENT_LENGTH, "0")
                .addHeader(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                .build();
        try
        {
            // 在这里 .get() 是正确的，因为要确保 "close" 信号被同步发送
            httpClient.executeRequest(req).get(); 
        } catch (Exception e)
        {
            logger.error("Failed to close http reader after {} retries, exception: {}", MAX_RETRIES, e.getMessage());
        }
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }
}