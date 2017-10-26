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
    private static final int MAX_ALLOWABLE_AMOUNT = 500;

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    private static class WireRequest implements Serializable {
        private String userId;
        private int amount;

        WireRequest(long millis) {
            Utils.sleep(millis);
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
    }

    private static boolean fraudDetect(WireRequest request) {
        String logMessage;

        boolean fraudulent = FRAUDULENT_USERS.contains(request.getUserId());

        if (fraudulent) {
            logMessage = String.format("Rejected fraudulent user %s", request.getUserId());
        } else {
            logMessage = String.format("Accepted request for $%d from user %s", request.getAmount(), request.getUserId());
        }

        System.out.println(logMessage);

        return !fraudulent;
    }

    private static boolean checkRequestAmount(WireRequest request) {
        boolean sufficientBalance = request.getAmount() < MAX_ALLOWABLE_AMOUNT;

        if (!sufficientBalance) System.out.println(String.format("Rejected excessive request of $%d", request.getAmount()));

        return sufficientBalance;
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        Streamlet<WireRequest> quietBranch = builder.newSource(() -> new WireRequest(20))
                .setNumPartitions(1)
                .setName("quiet-branch-requests")
                .filter(WireRequestsTopology::checkRequestAmount)
                .setName("quiet-branch-check-balance");
        Streamlet<WireRequest> mediumBranch = builder.newSource(() -> new WireRequest(10))
                .setNumPartitions(2)
                .setName("medium-branch-requests")
                .filter(WireRequestsTopology::checkRequestAmount)
                .setName("medium-branch-check-balance");
        Streamlet<WireRequest> busyBranch = builder.newSource(() -> new WireRequest(5))
                .setNumPartitions(4)
                .setName("busy-branch-requests")
                .filter(WireRequestsTopology::checkRequestAmount)
                .setName("busy-branch-check-balance");

        quietBranch
                .union(mediumBranch)
                .setNumPartitions(2)
                .setName("union-1")
                .union(busyBranch)
                .setName("union-2")
                .setNumPartitions(4)
                .filter(WireRequestsTopology::fraudDetect)
                .setName("all-branches-fraud-detect")
                .log();

        Config config = new Config();
        config.setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);
        config.setNumContainers(2);

        new Runner().run(args[0], config, builder);
    }
}