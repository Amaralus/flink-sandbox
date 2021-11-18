package amaralus.apps.flink.sandbox.bookexamples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BookAppCoProcess {

    private final StreamExecutionEnvironment environment;

    public BookAppCoProcess(StreamExecutionEnvironment environment) {
        this.environment = environment;
    }

    public void run() throws Exception {

        environment.getConfig().setAutoWatermarkInterval(0);

        var filterSwitches = environment.fromElements(
                Tuple2.of("sensor_2", 1000L),
                Tuple2.of("sensor_7", 6000L));


        environment.addSource(new SensorSource())
                .connect(filterSwitches)
                // общий айдишник для инфы сенсора и для инфы о времени сенсора (id=имя сенсора)
                .keyBy(SensorData::getId, switchTuple -> switchTuple.f0)
                .process(new SensorSourceFilter())
                .print();

        environment.execute("coProcess");
    }
}
