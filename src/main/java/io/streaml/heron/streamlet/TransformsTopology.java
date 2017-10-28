package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;
import io.streaml.heron.streamlet.utils.StreamletUtils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class TransformsTopology {
    public static void main(String[] args) throws Exception {
        Builder builder = Builder.createBuilder();

        builder.newSource(() -> {
                    Utils.sleep(100);
                    int rand = ThreadLocalRandom.current().nextInt(100);
                    System.out.println("Emitting random integer " + rand);
                    return rand;
                 })
                .transform(new SerializableTransformer<Integer, Integer>() {
                    private int count;

                    public void setup(Context context) {
                        count = (int) context.getState().getOrDefault("count", 0);
                        System.out.println("Read state: " + count);
                    }

                    public void transform(Integer i, Consumer<Integer> consumer) {
                        System.out.println("Incoming element: " + i);
                        i = i + 50;
                        consumer.accept(i);
                        System.out.println("New state: " + i);
                    }

                    public void cleanup() {}
                })
                .log();

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, new Config(), builder);
    }
}
