package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.dsl.Builder;
import com.twitter.heron.dsl.Config;
import com.twitter.heron.dsl.Runner;
import com.twitter.heron.dsl.Streamlet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class WireRequestsTopology {
    private static final List<String> USERS = Arrays.asList("honest-tina", "honest-jeff", "scheming-dave", "scheming-george");
    private static final List<String> FRAUDULENT_USERS = Arrays.asList("scheming-dave", "scheming-george");

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    private static class WireRequest implements Serializable {
        private String userId;
        private int amount;

        WireRequest() {
            Utils.sleep(10);
            this.userId = randomFromList(USERS);
            this.amount = ThreadLocalRandom.current().nextInt(1000);
            System.out.println(this.toString());
        }

        String getUserId() {
            return userId;
        }

        int getAmount() {
            return amount;
        }

        void setUserId(String userId) {
            this.userId = userId;
        }

        void setAmount(int amount) {
            this.amount = amount;
        }

        @Override
        public String toString() {
            return String.format("User: %s. Amount: %d.", userId, amount);
        }
    }

    private static boolean fraudDetect(WireRequest req) {
        boolean fraudulent = FRAUDULENT_USERS.contains(req.getUserId());

        if (fraudulent) System.out.println(String.format("Rejected fraudulent user %s", req.getUserId()));

        return !fraudulent;
    }

    private static boolean checkBalance(WireRequest req) {
        boolean sufficientBalance = req.getAmount() < 500;

        if (!sufficientBalance) System.out.println(String.format("Rejected excessive request of %d", req.getAmount()));

        return sufficientBalance;
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        Streamlet<WireRequest> incomingWireRequests = builder.newSource(WireRequest::new)
                .setName("incoming-requests")
                .setNumPartitions(2);

        List<Streamlet<WireRequest>> forkedStream = incomingWireRequests.clone(2);

        Streamlet<WireRequest> stream1 = forkedStream.get(0)
                .filter(WireRequestsTopology::checkBalance)
                .setName("check-balance-stream-1")
                .repartition(1)
                .setName("reduce-to-1-partition-1")
                .filter(WireRequestsTopology::fraudDetect)
                .setName("fraud-detect-stream-1");

        Streamlet<WireRequest> stream2 = forkedStream.get(1)
                .filter(WireRequestsTopology::checkBalance)
                .setName("check-balance-stream-2")
                .repartition(1)
                .setName("reduce-to-1-partition-2")
                .filter(WireRequestsTopology::fraudDetect)
                .setName("fraud-detect-stream-2");

        Streamlet<WireRequest> reunitedStream = stream1.union(stream2)
                .setName("unite-streams")
                .repartition(2)
                .setName("increase-to-2-partitions");

        reunitedStream
                .repartition(1)
                .setName("reduce-back-to-1-partition")
                .log();

        Config config = new Config();
        config.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);
        config.setNumContainers(2);

        new Runner().run(args[0], config, builder);
    }
}
