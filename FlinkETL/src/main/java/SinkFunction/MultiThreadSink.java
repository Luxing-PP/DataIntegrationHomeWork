package SinkFunction;

import PO.SdkData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MultiThreadSink extends ClickHouseSinkFunction {
    // Client 线程的默认数量
    private final int DEFAULT_CLIENT_THREAD_NUM = 4;
    // 数据缓冲队列的默认容量
    private final int DEFAULT_QUEUE_CAPACITY = 5000;

    private ThreadPoolExecutor threadPoolExecutor;

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        threadPoolExecutor = new ThreadPoolExecutor(
                DEFAULT_CLIENT_THREAD_NUM,
                DEFAULT_CLIENT_THREAD_NUM,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
    }

    @Override
    public void invoke(SdkData value, Context context) throws Exception {
        threadPoolExecutor.execute(
                new ClientRunnable(value,conn)
        );
    }

    @Override
    public void close() throws Exception {
        super.close();
        threadPoolExecutor.shutdown();
    }
}
