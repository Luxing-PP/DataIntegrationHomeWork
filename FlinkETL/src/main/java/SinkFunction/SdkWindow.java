package SinkFunction;

import PO.SdkData;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SdkWindow implements AllWindowFunction<SdkData, Iterable<SdkData>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<SdkData> values, Collector<Iterable<SdkData>> out) throws Exception {
        out.collect(values);
    }
}
