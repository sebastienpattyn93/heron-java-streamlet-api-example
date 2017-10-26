package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.dsl.Builder;
import com.twitter.heron.dsl.Config;
import com.twitter.heron.dsl.Runner;
import com.twitter.heron.dsl.Streamlet;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class WireRequestsTopology {
    private static final int MAX_ALLOWABLE_BALANCE = 500;

    private static final List<String> USERS = Arrays.asList(
            "honest-tina",
            "honest-abe",
            "honest-ahmed",
            "scheming-dave"
    );

    private static final List<String> BLACKLISTED_USERS = Arrays.asList(
            "scheming-dave"
    );

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(ThreadLocalRandom.current().nextInt(ls.size()));
    }

    private static final class WireRequest {
        private final String userId;
        private final int amount;

        WireRequest(long sleepMillis) {
            Utils.sleep(sleepMillis);
            this.userId = randomFromList(USERS);
            this.amount = ThreadLocalRandom.current().nextInt(1000);
        }

        int getAmount() {
            return amount;
        }

        String getUserId() {
            return userId;
        }

        @Override
        public String toString() {
            return String.format("(user: %s, amount: %d)", userId, amount);
        }
    }

    private static boolean detectFraud(WireRequest request) {
        boolean trustedUser = !BLACKLISTED_USERS.contains(request.getUserId());

        if (!trustedUser) {
            System.out.println(
                    String.format("Untrusted user %s attempted to make a transaction and was rejected",
                            request.getUserId())
            );
        }

        return trustedUser;
    }

    private static boolean checkBalance(WireRequest request) {
        boolean sufficientBalance = request.getAmount() < MAX_ALLOWABLE_BALANCE;

        if (!sufficientBalance) {
            System.out.println(
                    String.format("User %s attempted to draw an excessive amount: $%d",
                            request.getUserId(),
                            request.getAmount()));
        }

        return request.getAmount() < MAX_ALLOWABLE_BALANCE;
    }

    public static void main(String[] args) {
        Builder builder = Builder.createBuilder();

        Streamlet<WireRequest> quietBranch = builder.newSource(() -> new WireRequest(20))
                .setName("quiet-branch-requests");
        Streamlet<WireRequest> mediumBranch = builder.newSource(() -> new WireRequest(10))
                .setName("medium-branch-requests");
        Streamlet<WireRequest> busyBranch = builder.newSource(() -> new WireRequest(5))
                .setName("busy-branch-requests");

        quietBranch
                .setNumPartitions(1)
                .filter(WireRequestsTopology::checkBalance)
                .setName("quiet-branch-check-balance");

        mediumBranch
                .setNumPartitions(2)
                .filter(WireRequestsTopology::checkBalance)
                .setName("medium-branch-check-balance");

        busyBranch
                .setNumPartitions(4)
                .filter(WireRequestsTopology::checkBalance)
                .setName("busy-branch-check-balance");

        quietBranch
                .union(mediumBranch)
                .setName("unite-quiet-and-medium")
                .union(busyBranch)
                .setName("unite-all")
                .filter(WireRequestsTopology::detectFraud)
                .setName("detect-fraud-all-branches")
                .log();

        Config config = new Config();

        new Runner().run(args[0], config, builder);
    }
}
