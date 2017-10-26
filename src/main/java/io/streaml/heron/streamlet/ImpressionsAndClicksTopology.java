package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ImpressionsAndClicksTopology {
    private static final List<String> ADS = Arrays.asList(
            "ford",
            "tesla",
            "chevy",
            "volkswagen"
    );

    private static final List<String> USERS = IntStream.range(1, 100)
            .mapToObj(i -> String.format("user%d", i))
            .collect(Collectors.toList());

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    private static class AdImpression implements Serializable {
        private String adId;
        private String userId;

        AdImpression() {
            Utils.sleep(ThreadLocalRandom.current().nextInt(10));
            this.adId = randomFromList(ADS);
            this.userId = randomFromList(USERS);
        }

        String getAdId() {
            return adId;
        }

        String getUserId() {
            return userId;
        }

        @Override
        public String toString() {
            return String.format("(ad: %s, user: %s)", adId, userId);
        }
    }

    private static class AdClick implements Serializable {
        private String adId;
        private String userId;

        AdClick() {
            Utils.sleep(ThreadLocalRandom.current().nextInt(10));
            this.adId = randomFromList(ADS);
            this.userId = randomFromList(USERS);
        }

        String getAdId() {
            return adId;
        }

        String getUserId() {
            return userId;
        }

        @Override
        public String toString() {
            return String.format("(ad: %s, user: %s)", adId, userId);
        }
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        KVStreamlet<String, String> impressions = builder.newSource(AdImpression::new)
                .setName("incoming-impressions")
                .mapToKV(i -> {
                    System.out.println(String.format("Incoming impression: %s", i));
                    return new KeyValue<>(i.getAdId(), i.getUserId());
                })
                .setName("map-impression-to-kv");

        KVStreamlet<String, String> clicks = builder.newSource(AdClick::new)
                .setName("incoming-clicks")
                .mapToKV(c -> {
                    System.out.println(String.format("Incoming click: %s", c));
                    return new KeyValue<>(c.getAdId(), c.getUserId());
                })
                .setName("map-click-to-kv");

        impressions
                .join(
                        clicks,
                        WindowConfig.TumblingTimeWindow(Duration.ofSeconds(5)),
                        (x, y) -> {
                            System.out.println(String.format("Joining %s and %s", x, y));
                            return (x.equals(y)) ? 1 : 0;
                        }
                )
                .setName("join-operation")
                .reduceByKeyAndWindow(
                        WindowConfig.TumblingTimeWindow(Duration.ofSeconds(5)),
                        (cumulative, i) -> {
                            int sum = cumulative + i;
                            System.out.println(String.format("Sum: %d", sum));
                            return sum;
                        }
                )
                .setName("reduce-operation")
                .log();

        new Runner().run(args[0], new Config(), builder);
    }
}
