package io.streaml.heron.streamlet.utils;

import java.util.List;
import java.util.Random;

public class StreamletUtils {
    public static String getTopologyName(String[] args) throws Exception {
        if (args.length == 0) {
            throw new Exception("You must supply a name for the topology");
        } else {
            return args[0];
        }
    }

    public static <T> T randomFromList(List<T> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }

    public static int getParallelism(String[] args, int defaultParallelism) {
        return (args.length > 1) ? Integer.parseInt(args[1]) : defaultParallelism;
    }
}
