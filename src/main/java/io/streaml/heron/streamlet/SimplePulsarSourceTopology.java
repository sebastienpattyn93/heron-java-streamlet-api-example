package io.streaml.heron.streamlet;

import com.twitter.heron.streamlet.*;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.UnsupportedEncodingException;

public class SimplePulsarSourceTopology {
    private static class PulsarSource implements Source<String> {
        private PulsarClient client;
        private Consumer consumer;
        private String consumeTopic;

        PulsarSource(String topic) {
            this.consumeTopic = topic;
        }

        public void setup(Context context) {
            try {
                client = PulsarClient.create("pulsar://localhost:6650");
                consumer = client.subscribe(consumeTopic, "test-subscription");
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

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        builder.newSource(new PulsarSource("persistent://sample/standalone/ns1/heron-pulsar-test-topic"))
                .log();

        new Runner().run(args[0], new Config(), builder);
    }
}
