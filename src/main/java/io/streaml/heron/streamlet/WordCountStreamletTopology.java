package io.streaml.heron.streamlet;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public final class WordCountStreamletTopology {
    private static final Logger LOG = Logger.getLogger(WordCountStreamletTopology.class.getName());
    private static final float CPU = 2.0f;
    private static final long GIGABYTES_OF_RAM = 6;
    private static final int NUM_CONTAINERS = 2;

    private static final List<String> SENTENCES_1 = Arrays.asList(
            "I have nothing to declare but my genius",
            "You can even",
            "Compassion is an action word with no boundaries",
            "To thine own self be true"
    );

    private static final List<String> SENTENCES_2 = Arrays.asList(
            "Is this the real life? Is this just fantasy?"
    );

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(ThreadLocalRandom.current().nextInt(ls.size()));
    }

    private WordCountStreamletTopology() {
    }

    public static void main(String[] args) throws Exception {
        Builder builder = Builder.createBuilder();

        Streamlet<String> randomSentences1 = builder.newSource(() -> randomFromList(SENTENCES_1));
        Streamlet<String> randomSentences2 = builder.newSource(() -> randomFromList(SENTENCES_2));

        randomSentences1
                .union(randomSentences2)
                .map(sentence -> sentence.replace("?", ""))
                .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
                .reduceByKeyAndWindow(
                        word -> word,
                        word -> 1,
                        WindowConfig.TumblingCountWindow(30),
                        (x, y) -> x + y
                )
                .consume(kv -> {
                    String logMessage = String.format("(word: %s, count: %d)",
                            kv.getKey().getKey(),
                            kv.getValue()
                    );
                    LOG.info(logMessage);
                });

        Resources resources = new Resources.Builder()
                .setCpu(CPU)
                .setRam(ByteAmount.fromGigabytes(GIGABYTES_OF_RAM).asBytes())
                .build();

        Config config = new Config.Builder()
                .setNumContainers(NUM_CONTAINERS)
                .setDeliverySemantics(applyDeliverySemantics(args))
                .setContainerResources(resources)
                .build();

        String topologyName;

        if (args.length == 0) {
            throw new Exception("You must supply a name for the topology");
        } else {
            topologyName = args[0];
        }

        new Runner().run(topologyName, config, builder);
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
