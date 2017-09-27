package io.streaml.heron.functionalapi;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import com.twitter.heron.dsl.*;

public final class IntegerProcessingFunctionalTopology {
    private IntegerProcessingFunctionalTopology() {
    }

    private static int randomInt(int lowerBound, int upperBound) {
        return ThreadLocalRandom.current().nextInt(lowerBound, upperBound + 1);
    }

    public static void main(String[] args) {
        Builder builder = Builder.CreateBuilder();

        Streamlet<Integer> zeroes = builder.newSource(() -> 0);

        builder.newSource(() -> randomInt(1, 10))
                .setNumPartitions(3)
                .map(i -> i + 1)
                .union(zeroes)
                .filter(i -> i != 2)
                .log();

        Config conf = new Config();
        conf.setNumContainers(2);
        conf.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);

        Runner runner = new Runner();
        runner.run(args[0], conf, builder);
    }
}
