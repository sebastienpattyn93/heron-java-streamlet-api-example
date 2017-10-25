package io.streaml.heron.streamlet;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.dsl.*;

import java.util.concurrent.ThreadLocalRandom;

public final class IntegerProcessingTopology {
    private static final float CPU = 2.0f;
    private static final long GIGABYTES_OF_RAM = 6;
    private static final int NUM_CONTAINERS = 2;

    private IntegerProcessingTopology() {
    }

    public static void main(String[] args) throws Exception {
        Builder builder = Builder.createBuilder();

        Streamlet<Integer> zeroes = builder.newSource(() -> 0);

        builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
                .setNumPartitions(5)
                .setName("random-ints")
                .map(i -> i + 1)
                .setName("add-one")
                .repartition(2)
                .setName("repartition")
                .union(zeroes)
                .setName("unify-streams")
                .filter(i -> i != 2)
                .setName("remove-twos")
                .log();

        Config conf = new Config();
        conf.setNumContainers(NUM_CONTAINERS);
        conf.setDeliverySemantics(applyDeliverySemantics(args));

        Resources resources = new Resources();
        resources.withCpu(CPU);
        resources.withRam(ByteAmount.fromGigabytes(GIGABYTES_OF_RAM).asBytes());
        conf.setContainerResources(resources);

        String topologyName;

        if (args.length == 0) {
            throw new Exception("You must supply a name for the topology");
        } else {
            topologyName = args[0];
        }

        new Runner().run(topologyName, conf, builder);
    }

    private static Config.DeliverySemantics applyDeliverySemantics(String[] args) throws Exception {
        if (args.length > 1) {
            switch(args[1]) {
                case "at-most-once":
                    System.out.println("Applying at-most-once semantics");
                    return Config.DeliverySemantics.ATMOST_ONCE;
                case "at-least-once":
                    System.out.println("Applying at-least-once semantics");
                    return Config.DeliverySemantics.ATLEAST_ONCE;
                case "effectively-once":
                    System.out.println("Applying effectively-once semantics");
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