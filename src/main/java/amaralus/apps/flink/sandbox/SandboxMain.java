package amaralus.apps.flink.sandbox;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SandboxMain {

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                "this is first sentence",
                "but i want more strings")
                .flatMap((sentence, collector) -> { for (var word : sentence.split(" ")) collector.collect(word); }, Types.STRING)
                .keyBy(str -> str.charAt(0))
                .reduce((prev, current) -> prev + " " + current)
                .print();

        env.execute("flat map");
    }
}

