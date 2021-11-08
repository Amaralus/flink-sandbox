package amaralus.apps.flink.sandbox;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static java.time.Duration.ofSeconds;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forBoundedOutOfOrderness;

public class SandboxMain {

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                "this is first sentence",
                "but i want more strings")
                .flatMap((sentence, collector) -> { for (var word : sentence.split(" ")) collector.collect(word); },
                        TypeInformation.of(String.class))
                .keyBy(str -> str.charAt(0))
                .reduce((prev, current) -> prev + " " + current)
                .print();

        env.execute("flat map");
    }

    public static void exampleStream(StreamExecutionEnvironment env) throws Exception {
        env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(forBoundedOutOfOrderness(ofSeconds(1)))
                .map(SandboxMain::toCelsius)
                .keyBy(SensorData::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(SandboxMain::apply)
                .print();

        env.execute("Compute avg temp");
    }

    public static SensorData toCelsius(SensorData sensorData) {
        var celsius = (sensorData.getTemperature() - 32) * (5.0d / 9.0d);
        return new SensorData(sensorData.getId(), sensorData.getTimestamp(), celsius);
    }

    public static void apply(String sensorId, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) {
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

