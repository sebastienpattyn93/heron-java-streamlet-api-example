package io.streaml.heron.streamlet;

import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SmartWatchTopology {
    private static final List<String> JOGGERS = IntStream.range(1, 100)
            .mapToObj(i -> String.format("jogger%d", i))
            .collect(Collectors.toList());

    private static class WatchReading {
        private String joggerId;

        WatchReading() {
            this.joggerId = randomFromList(JOGGERS);
        }
    }

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        builder.newSource(WatchReading::new)
                .log();

        Config config = new Config();

        new Runner().run(args[0], config, builder);
    }
}
