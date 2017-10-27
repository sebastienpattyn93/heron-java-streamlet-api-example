package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ImpressionsAndClicksTopology {
    private static final Logger LOG = Logger.getLogger(ImpressionsAndClicksTopology.class.getName());

    private static final List<String> ADS = Arrays.asList(
            "acme",
            "blockchain-inc",
            "omnicorp"
    );

    private static final List<String> USERS = IntStream.range(1, 100)
            .mapToObj(i -> String.format("user%d", i))
            .collect(Collectors.toList());

    private static class AdImpression implements Serializable {
        private String adId;
        private String userId;
        private String impressionId;

        AdImpression() {
            Utils.sleep(500);
            this.adId = HeronStreamletUtils.randomFromList(ADS);
            this.userId = HeronStreamletUtils.randomFromList(USERS);
            this.impressionId = UUID.randomUUID().toString();
            LOG.info(String.format("Emitting impression: %s", this));
        }

        String getAdId() {
            return adId;
        }

        String getUserId() {
            return userId;
        }

        @Override
        public String toString() {
            return String.format(
                    "(adId; %s, impressionId: %s)",
                    adId,
                    impressionId
            );
        }
    }

    private static class AdClick implements Serializable {
        private String adId;
        private String userId;
        private String clickId;

        AdClick() {
            Utils.sleep(500);
            this.adId = HeronStreamletUtils.randomFromList(ADS);
            this.userId = HeronStreamletUtils.randomFromList(USERS);
            this.clickId = UUID.randomUUID().toString();
            LOG.info(String.format("Emitting click: %s", this));
        }

        String getAdId() {
            return adId;
        }

        String getUserId() {
            return userId;
        }

        @Override
        public String toString() {
            return String.format(
                    "(adId; %s, clickId: %s)",
                    adId,
                    clickId
            );
        }
    }

    private static int incrementIfSameUser(String userId1, String userId2) {
        return (userId1.equals(userId2)) ? 1 : 0;
    }

    private static int countCumulativeClicks(int cumulative, int incoming) {
        return cumulative + incoming;
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        KVStreamlet<String, String> impressions = processingGraphBuilder.newSource((AdImpression::new))
                .mapToKV(impression -> new KeyValue<>(impression.getAdId(), impression.getUserId()));
        KVStreamlet<String, String> clicks = processingGraphBuilder.newSource(AdClick::new)
                .mapToKV(click -> new KeyValue<>(click.getAdId(), click.getUserId()));

        impressions
                .join(clicks, WindowConfig.TumblingCountWindow(100), ImpressionsAndClicksTopology::incrementIfSameUser)
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(200), ImpressionsAndClicksTopology::countCumulativeClicks)
                .log();

        Config config = new Config();

        String topologyName = HeronStreamletUtils.getTopologyName(args);

        // Finally, convert the processing graph and configuration into a Heron topology
        // and run it in a Heron cluster.
        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
