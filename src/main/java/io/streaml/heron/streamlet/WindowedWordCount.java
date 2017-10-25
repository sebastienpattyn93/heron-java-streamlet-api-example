package io.streaml.heron.streamlet;

import com.twitter.heron.dsl.*;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class WindowedWordCount {
    private static final List<String> SENTENCES = Arrays.asList(
            "mary had a little lamb",
            "i have nothing to declare but my genius",
            "baseball is 90% mental and 50% physical",
            "you can even"
    );

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    private static List<String> splitIntoWords(String sentence) {
        return Arrays.asList(sentence.split("\\s+");
    }

    private static KeyValue<String, Integer> increment(String word) {
        return new KeyValue<>(word, 1);
    }

    private static int reduce(int currentCount, int numOccurrences) {
        return currentCount + numOccurrences;
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        builder.newSource(() -> randomFromList(SENTENCES))
                .setName("incoming-sentences")
                .flatMap(WindowedWordCount::splitIntoWords)
                .setName("split-words")
                .mapToKV(WindowedWordCount::increment)
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(100), WindowedWordCount::reduce)
                .setName("count-words")
                .log();

        Config config = new Config();
        config.setNumContainers(2);

        new Runner().run(args[0], config, builder);
    }
}
