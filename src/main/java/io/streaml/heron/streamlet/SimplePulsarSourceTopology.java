package io.streaml.heron.streamlet;

import com.twitter.heron.streamlet.*;
import io.streaml.heron.streamlet.utils.StreamletUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.UnsupportedEncodingException;

public class SimplePulsarSourceTopology {
    private static class PulsarSource implements Source<String> {
        private PulsarClient client;
        private Consumer consumer;
        private String pulsarConnectionUrl;
        private String consumeTopic;
        private String subscription;

        PulsarSource(String url, String topic, String subscription) {
            this.pulsarConnectionUrl = url;
            this.consumeTopic = topic;
            this.subscription = subscription;
        }

        public void setup(Context context) {
            try {
                client = PulsarClient.create(pulsarConnectionUrl);
                consumer = client.subscribe(consumeTopic, subscription);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }

        public String get() {
            try {
                return new String(consumer.receive().getData(), "utf-8");
            } catch (PulsarClientException | UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        public void cleanup() {}
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        Source<String> pulsarSource = new PulsarSource(
                "pulsar://localhost:6650", // Pulsar connection URL
                "persistent://sample/standalone/ns1/heron-pulsar-test-topic", // Pulsar topic
                "subscription-1" // Subscription name for Pulsar topic
        );

        processingGraphBuilder.newSource(pulsarSource)
                .log();

        Config config = new Config();
        config.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
