package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;
import io.streaml.heron.streamlet.utils.StreamletUtils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class TransformsTopology {
    private static class DoNothingTransformer<T> implements SerializableTransformer<T, T> {
        public void setup(Context context) {}

        public void transform(T in, Consumer<T> consumer) {
            consumer.accept(in);
        }

        public void cleanup() {}
    }

    private static class IncrementTransformer implements SerializableTransformer<Integer, Integer> {
        private int increment;

        public IncrementTransformer(int increment) {
            this.increment = increment;
        }

        public void setup(Context context) {
        }

        public void transform(Integer in, Consumer<Integer> consumer) {
            int incrementedValue = in + increment;
            consumer.accept(incrementedValue);
        }

        public void cleanup() {}
    }

    public static void main(String[] args) throws Exception {
        Builder builder = Builder.createBuilder();

        builder.newSource(() -> ThreadLocalRandom.current().nextInt(100))
                .transform(new DoNothingTransformer<>())
                .transform(new IncrementTransformer(10))
                .transform(new IncrementTransformer(-7))
                .transform(new DoNothingTransformer<>())
                .transform(new IncrementTransformer(-3))
                .log();

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, new Config(), builder);
    }
}
