package amaralus.apps.flink.sandbox.bookexamples;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class SensorSourceFilter extends CoProcessFunction<SensorData, Tuple2<String, Long>, SensorData> {

    // доступ к переменным будет по ключу (будет общий у сенсорДата и кортежа)
    private transient ValueState<Boolean> forwardingEnabled;
    private transient ValueState<Long> disableTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN));
        disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
    }

    @Override
    public void processElement1(SensorData sensorData, Context ctx, Collector<SensorData> out) throws Exception {
        var forward = forwardingEnabled.value();
        if (forward != null && forward) {
            System.out.println("sensor data passed");
            out.collect(sensorData);
        }
    }

    @Override
    public void processElement2(Tuple2<String, Long> sensorTime, Context ctx, Collector<SensorData> out) throws Exception {
        forwardingEnabled.update(true);

        var timerTimestamp = ctx.timerService().currentProcessingTime() + sensorTime.f1;
        var curTimerTimestamp = disableTimer.value();

        // если таймера нет или новый таймер должен закончиться позже текущего
        if (curTimerTimestamp == null || timerTimestamp > curTimerTimestamp) {
            if (curTimerTimestamp != null)
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);

            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            disableTimer.update(timerTimestamp);
            System.out.println("timer registered for sensor=" + sensorTime.f0 + " at time=" + sensorTime.f1);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorData> out) throws Exception {
        System.out.println("on timer");
        forwardingEnabled.clear();
        disableTimer.clear();
    }
}
