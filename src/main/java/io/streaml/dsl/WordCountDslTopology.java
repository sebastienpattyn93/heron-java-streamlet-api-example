package io.streaml.dsl;

import java.util.Arrays;

import com.twitter.heron.dsl.*;

public final class WordCountDslTopology {
    private WordCountDslTopology() {
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("Specify topology name");
        }

        int parallelism = 1;
        if (args.length > 1) {
            parallelism = Integer.parseInt(args[1]);
        }
        Builder builder = Builder.CreateBuilder();
        builder.newSource(() -> "Mary had a little lamb")
                .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
                .mapToKV((word) -> new KeyValue<>(word, 1))
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10), (x, y) -> x + y)
                .log();
        Config conf = new Config();
        conf.setNumContainers(parallelism);
        Runner runner = new Runner();
        runner.run(args[0], conf, builder);
    }
}
