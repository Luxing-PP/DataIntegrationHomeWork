package SinkFunction;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class ClickHouseRowConverter extends AbstractJdbcRowConverter {
    public ClickHouseRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return null;
    }
}
