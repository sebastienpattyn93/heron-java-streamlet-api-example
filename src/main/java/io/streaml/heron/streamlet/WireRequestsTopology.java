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
    private static final List<String> USERS = Arrays.asList("honest-tina", "honest-jeff", "scheming-dave", "scheming-linda");
    private static final List<String> FRAUDULENT_USERS = Arrays.asList("scheming-dave", "scheming-linda");

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
            return String.format("Accepted request for $%d from user %s", amount, userId);
        }
    }

    private static boolean fraudDetect(WireRequest req) {
        boolean fraudulent = FRAUDULENT_USERS.contains(req.getUserId());

        if (fraudulent) System.out.println(String.format("Rejected fraudulent user %s", req.getUserId()));

        return !fraudulent;
    }

    private static boolean checkBalance(WireRequest req) {
        boolean sufficientBalance = req.getAmount() < 500;

        if (!sufficientBalance) System.out.println(String.format("Rejected excessive request of $%d", req.getAmount()));

        return sufficientBalance;
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        Streamlet<WireRequest> branch1 = builder.newSource(WireRequest::new)
                .setNumPartitions(1)
                .filter(WireRequestsTopology::checkBalance)
                .setNumPartitions(2);

        Streamlet<WireRequest> branch2 = builder.newSource(WireRequest::new)
                .setNumPartitions(3)
                .filter(WireRequestsTopology::checkBalance)
                .setNumPartitions(2);

        Streamlet<WireRequest> branch3 = builder.newSource(WireRequest::new)
                .setNumPartitions(5)
                .filter(WireRequestsTopology::checkBalance)
                .setNumPartitions(2);

        branch1
                .union(branch2)
                .union(branch3)
                .filter(WireRequestsTopology::fraudDetect)
                .log();

        Config config = new Config();
        config.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);
        config.setNumContainers(2);

        new Runner().run(args[0], config, builder);
    }
}
