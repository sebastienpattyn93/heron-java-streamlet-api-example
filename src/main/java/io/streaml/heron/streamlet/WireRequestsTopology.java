package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;
import com.twitter.heron.streamlet.Streamlet;

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

        WireRequest(int sleep) {
            Utils.sleep(10);
            this.userId = randomFromList(USERS);
            this.amount = ThreadLocalRandom.current().nextInt(1000);
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

        Streamlet<WireRequest> quietBranch = builder.newSource(() -> new WireRequest(20))
                .setNumPartitions(1)
                .filter(WireRequestsTopology::checkBalance);

        Streamlet<WireRequest> mediumBranch = builder.newSource(() -> new WireRequest(10))
                .setNumPartitions(3)
                .filter(WireRequestsTopology::checkBalance);

        Streamlet<WireRequest> busyBranch = builder.newSource(() -> new WireRequest(3))
                .setNumPartitions(5)
                .filter(WireRequestsTopology::checkBalance);

        quietBranch
                .union(mediumBranch)
                .union(busyBranch)
                .filter(WireRequestsTopology::fraudDetect)
                .setNumPartitions(5)
                .log();

        Config config = new Config();
        config.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);
        config.setNumContainers(2);

        new Runner().run(args[0], config, builder);
    }
}
