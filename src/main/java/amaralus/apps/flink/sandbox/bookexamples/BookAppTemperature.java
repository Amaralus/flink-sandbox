package amaralus.apps.flink.sandbox.bookexamples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static java.time.Duration.ofSeconds;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forBoundedOutOfOrderness;

public class BookAppTemperature {

    private final StreamExecutionEnvironment environment;

    public BookAppTemperature(StreamExecutionEnvironment environment) {
        this.environment = environment;
    }

    public void run() throws Exception {
        environment.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(forBoundedOutOfOrderness(ofSeconds(1)))
                .map(this::toCelsius)
                .keyBy(SensorData::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(this::apply)
                .print();

        environment.execute("Compute avg temp");
    }

    private SensorData toCelsius(SensorData sensorData) {
        var celsius = (sensorData.getTemperature() - 32) * (5.0d / 9.0d);
        return new SensorData(sensorData.getId(), sensorData.getTimestamp(), celsius);
    }

    private void apply(String sensorId, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) {
        int cnt = 1;
        double sum = 0.0;
        for (var sensorData : input) {
            sum += sensorData.getTemperature();
            ++cnt;
        }
        double avgTemp = sum / cnt;

        out.collect(new SensorData(sensorId, window.getEnd(), avgTemp));
    }
}
