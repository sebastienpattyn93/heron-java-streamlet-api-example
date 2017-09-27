package io.streaml.heron.functionalapi;

import java.util.Arrays;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.dsl.*;

public final class WordCountFunctionalTopology {
    private static final float CPU = 1.0f;
    private static final long MEGS_RAM = 1024;
    private static final int NUM_CONTAINERS = 2;

    private WordCountFunctionalTopology() {
    }

    private static boolean isEffectivelyOnce(String[] args) {
        return (args.length > 1 && args[1].equals("effectively-once"));
    }

    public static void main(String[] args) throws Exception {
        Builder builder = Builder.CreateBuilder();
        builder.newSource(() -> "Mary had a little lamb")
                .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
                .mapToKV((word) -> new KeyValue<>(word, 1))
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10), (x, y) -> x + y)
                .log();

        Config conf = new Config();
        conf.setNumContainers(NUM_CONTAINERS);

        Resources resources = new Resources();
        resources.withCpu(CPU);
        resources.withRam(ByteAmount.fromMegabytes(MEGS_RAM).asMegabytes());

        if (isEffectivelyOnce(args)) {
            conf.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);
            System.out.println("Running topology with effectively-once semantics");
        }

        Runner runner = new Runner();

        String topologyName;

        if (args.length == 0) {
            throw new Exception("You must supply a name for the topology");
        } else {
            topologyName = args[0];
        }

        runner.run(topologyName, conf, builder);
    }
}
