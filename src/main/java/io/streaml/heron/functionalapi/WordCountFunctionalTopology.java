package io.streaml.heron.functionalapi;

import java.util.Arrays;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.dsl.Builder;
import com.twitter.heron.dsl.Config;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.Resources;
import com.twitter.heron.dsl.Runner;
import com.twitter.heron.dsl.WindowConfig;

public final class WordCountFunctionalTopology {
    private static final float CPU = 1.0f;
    private static final long MEGS_RAM = 1024;
    private static final int NUM_CONTAINERS = 2;

    private WordCountFunctionalTopology() {
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
        conf.setDeliverySemantics(deliverySemantics(args));

        Resources resources = new Resources();
        resources.withCpu(CPU);
        resources.withRam(ByteAmount.fromMegabytes(MEGS_RAM).asMegabytes());

        String topologyName;

        if (args.length == 0) {
            throw new Exception("You must supply a name for the topology");
        } else {
            topologyName = args[0];
        }

        new Runner().run(topologyName, conf, builder);
    }

    private static Config.DeliverySemantics deliverySemantics(String[] args) throws Exception {
        if (args.length > 1) {
            switch(args[1]) {
                case "at-most-once":
                    return Config.DeliverySemantics.ATMOST_ONCE;
                case "at-least-once":
                    return Config.DeliverySemantics.ATLEAST_ONCE;
                case "effectively-once":
                    return Config.DeliverySemantics.EFFECTIVELY_ONCE;
                default:
                    throw new Exception("You've selected a delivery semantics that is not amongst the available options: " +
                            "at-most-once, at-least-once, effectively-once");
            }
        } else {
            return Config.DeliverySemantics.ATLEAST_ONCE;
        }
    }
}
