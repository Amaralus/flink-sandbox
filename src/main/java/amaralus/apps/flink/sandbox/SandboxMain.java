package amaralus.apps.flink.sandbox;

import amaralus.apps.flink.sandbox.bookexamples.BookAppCoProcess;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SandboxMain {

    public static void main(String[] args) throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();

        new BookAppCoProcess(environment).run();
    }

    private static void simpleProcessing(StreamExecutionEnvironment environment) throws Exception {
        environment.fromElements(
                "this is first sentence",
                "but i want more strings")
                .flatMap((sentence, collector) -> { for (var word : sentence.split(" ")) collector.collect(word); }, Types.STRING)
                .keyBy(str -> str.charAt(0))
                .reduce((prev, current) -> prev + " " + current)
                .print();

        environment.execute("flat map");
    }
}

