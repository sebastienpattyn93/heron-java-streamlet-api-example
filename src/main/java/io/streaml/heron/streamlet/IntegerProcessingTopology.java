package io.streaml.heron.streamlet;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.*;
import io.streaml.heron.streamlet.utils.StreamletUtils;

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

        Resources resources = new Resources();
        resources.withCpu(CPU);
        resources.withRam(ByteAmount.fromGigabytes(GIGABYTES_OF_RAM).asBytes());
        conf.setContainerResources(resources);

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, conf, builder);
    }
}
