import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestClickHouseConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql(" " +
                " CREATE TABLE sourceTable ( " +
                "  a varchar, " +
                "  b varchar " +
                " ) WITH ( " +
                "  'connector.type' = 'kafka', " +
                "  'connector.version' = 'universal', " +
                "  'connector.topic' = 'flink_source', " +
                "  'connector.startup-mode' = 'earliest-offset', " +
                "  'connector.properties.zookeeper.connect' = '127.0.0.1:2181', " +
                "  'connector.properties.bootstrap.servers' = '127.0.0.1:9092', " +
                "  'format.type' = 'json' " +
                " ) ");
        tableEnvironment.executeSql("CREATE TABLE sinkTable (" +
                "    a String," +
                "    b String" +
                ") WITH (" +
                "    'connector' = 'clickhouse'," +
                "    'url' = 'jdbc:clickhouse://127.0.0.1:8123/nicetuan_webapp'," +
                "    'table-name' = 'dwd_user_login'," +
                "    'username' = 'default'," +
                "    'password' = ''," +
                "    'format' = 'json'" +
                ")");
        tableEnvironment.executeSql(
                "insert into sinkTable " +
                        "select * " +
                        "from sourceTable");
    }
}