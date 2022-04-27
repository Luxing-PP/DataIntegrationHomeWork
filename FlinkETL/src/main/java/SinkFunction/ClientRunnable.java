package SinkFunction;

import PO.SdkData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.sql.SQLException;

public class ClientRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClientRunnable.class);
    private SdkData value;
    private ClickHouseConnection conn;
    private ClickHouseStatement stmt;

    public ClientRunnable(SdkData value, ClickHouseConnection conn){
        this.value = value;
        this.conn = conn;
    }

    @Override
    public void run() {
        String sql = value.resolveSql();
        try{
            stmt = conn.createStatement();
            stmt.executeQuery(sql);
        }catch (Exception e){
            logger.error(sql);
            logger.error(value.getEventBody());
            e.printStackTrace();
        }finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
