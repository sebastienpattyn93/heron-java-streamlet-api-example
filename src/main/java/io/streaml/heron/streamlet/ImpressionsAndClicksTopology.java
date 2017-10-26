package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.dsl.*;

import java.io.Serializable;
import java.security.Key;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
            Utils.sleep(10);
            this.adId = randomFromList(ADS);
            this.userId = randomFromList(USERS);
        }

        String getAdId() {
            return adId;
        }

        String getUserId() {
            return userId;
        }
    }

    private static class AdClick implements Serializable {
        private String adId;
        private String userId;

        AdClick() {
            Utils.sleep(10);
            this.adId = randomFromList(ADS);
            this.userId = randomFromList(USERS);
        }

        String getAdId() {
            return adId;
        }

        String getUserId() {
            return userId;
        }
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        KVStreamlet<String, String> impressions = builder.newSource(AdImpression::new)
                .setName("incoming-impressions")
                .mapToKV(i -> new KeyValue<>(i.getAdId(), i.getUserId()));

        KVStreamlet<String, String> clicks = builder.newSource(AdClick::new)
                .setName("incoming-clicks")
                .mapToKV(c -> new KeyValue<>(c.getAdId(), c.getUserId()));

        impressions
                .join(
                        clicks,
                        WindowConfig.TumblingCountWindow(100),
                        (x, y) -> (x.equals(y)) ? 1 : 0
                )
                .reduceByKeyAndWindow(
                        WindowConfig.TumblingCountWindow(100),
                        (cum, i) -> cum + i
                )
                .log();

        new Runner().run(args[0], new Config(), builder);
    }
}
