import PO.SdkData;
import SinkFunction.ClickHouseSinkFunction;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class client {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Map<String, String> globalParameters = new HashMap<>();
        // ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://114.212.241.8:8123/");
        // sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "1");
        globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "d:/");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "2");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.readTextFile("/root/data/record.txt","utf-8");
//        DataStream<String> inputStream = env.readTextFile("D:\\DATA\\DataIntegrate\\record.txt","utf-8");
//        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // Transform 操作
        SingleOutputStreamOperator<SdkData> dataStream = inputStream.map((MapFunction<String, SdkData>) input ->{
            input = input.substring(6);
            try {
                SdkData sdkData = JSON.parseObject(input, SdkData.class);
                return sdkData;
            }catch (Exception e){
                e.printStackTrace();
                System.err.println(input);
            }

            return null;
        });

        // create props for sink
        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "default.user_table");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10000");

        dataStream.addSink(new ClickHouseSinkFunction());
        dataStream.print();
//        ClickHouseSink sink = new ClickHouseSink(props);
//        dataStream.addSink(sink);


        env.execute("clickhouse sink test");
    }
}
