package io.streaml.heron.streamlet;

import java.util.List;
import java.util.Random;

class HeronStreamletUtils {
    static String getTopologyName(String[] args) throws Exception {
        if (args.length == 0) {
            throw new Exception("You must supply a name for the topology");
        } else {
            return args[0];
        }
    }

    static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    static int getParallelism(String[] args, int defaultParallelism) {
        return (args.length > 1) ? Integer.parseInt(args[1]) : defaultParallelism;
    }
}
