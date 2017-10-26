package io.streaml.heron.streamlet;

import com.twitter.heron.streamlet.Builder;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class WireRequestsTopology {
    private static final List<String> USERS = Arrays.asList(
            "honest-tina",
            "honest-abe",
            "honest-ahmed",
            "scheming-dave"
    );

    private static <T> T randomFromList(List<T> ls) {
        return ls.get(ThreadLocalRandom.current().nextInt(ls.size()));
    }

    private static class WireRequest {
        private String userId;
        private int amount;

        WireRequest() {
            this.userId = randomFromList(USERS);
            this.amount = ThreadLocalRandom.current().nextInt(1000);
        }
    }

    public static void main(String[] args) {
        Builder
    }
}
