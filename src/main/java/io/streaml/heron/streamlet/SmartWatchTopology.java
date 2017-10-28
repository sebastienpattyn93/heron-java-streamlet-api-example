package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;
import io.streaml.heron.streamlet.utils.StreamletUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class SmartWatchTopology {
    private static final Logger LOG = Logger.getLogger(SmartWatchTopology.class.getName());

    private static final List<String> JOGGERS = Arrays.asList(
            "bill",
            "ted"
    );

    private static class SmartWatchReading implements Serializable {
        private final String userId;
        private final float distanceRun;


        SmartWatchReading() {
            Utils.sleep(5);
            this.userId = StreamletUtils.randomFromList(JOGGERS);
            this.distanceRun = (float) ThreadLocalRandom.current().nextInt(10);
            LOG.info(String.format("Emitted smart watch reading: %s", this));
        }

        KeyValue<String, Float> toKV() {
            return new KeyValue<>(userId, distanceRun);
        }

        @Override
        public String toString() {
            return String.format("(user: %s, distance: %f)", userId, distanceRun);
        }
    }

    public static void main(String[] args) throws Exception {
        int jogLength = 20;

        Builder processingGraphBuilder = Builder.createBuilder();

        processingGraphBuilder.newSource(SmartWatchReading::new)
                .setName("smart-watch-readings-source")
                .mapToKV(SmartWatchReading::toKV)
                .setName("map-smart-watch-readings-to-kv")
                .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(jogLength), (x, y) -> (x + y) / jogLength)
                .setName("emit-average-speed-by-runner")
                .log();

        Config config = new Config();

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}