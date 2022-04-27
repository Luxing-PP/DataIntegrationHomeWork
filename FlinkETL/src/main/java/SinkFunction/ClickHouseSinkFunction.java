package SinkFunction;

import PO.SdkData;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseSinkFunction extends RichSinkFunction<SdkData> {
    private static final long serialVersionUID = 1L;

    private static final String MAX_PARALLEL_REPLICAS_VALUE = "2";

    protected ClickHouseConnection conn;
    private ClickHouseStatement stmt;

    public ClickHouseSinkFunction() {

    }

    @Override
    public void open(Configuration parameters) {
        ClickHouseProperties properties = new ClickHouseProperties();
        BalancedClickhouseDataSource dataSource;
        try {
            if (null == conn) {
                dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://114.212.241.8:8123", properties);
                conn = dataSource.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(SdkData value, Context context) throws Exception {
        InputStream result = new ByteArrayInputStream(value.getEventBody().getBytes(StandardCharsets.UTF_8));
        String sql = value.resolveSql();
//        System.out.println(sql);
        try{
            stmt = conn.createStatement();
            stmt.executeQuery(sql);
        }catch (Exception e){
            System.out.println(sql);
            System.out.println(value.getEventBody());
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
