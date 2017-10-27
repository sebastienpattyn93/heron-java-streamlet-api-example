package io.streaml.heron.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.streamlet.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

public class FilesystemSinkTopology {
    private static class FilesystemSink<T> implements Sink<T> {
        private Path tempFilePath;
        private File tempFile;

        FilesystemSink(File f) {
            this.tempFile = f;
        }

        public void setup(Context context) {
            this.tempFilePath = Paths.get(tempFile.toURI());
        }

        public void put(T element) {
            byte[] bytes = String.format("%s\n", element.toString()).getBytes();

            try {
                Files.write(tempFilePath, bytes, StandardOpenOption.APPEND);
                System.out.println(
                        String.format("Wrote %s to %s", new String(bytes), tempFilePath.toAbsolutePath())
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void cleanup() {
        }
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        File f = File.createTempFile("filesystem-sink-example", ".tmp");
        System.out.println(String.format("Ready to write to file %s", f.getAbsolutePath()));

        processingGraphBuilder.newSource(() -> {
                    Utils.sleep(500);
                    return ThreadLocalRandom.current().nextInt(100);
                })
                .toSink(new FilesystemSink<>(f));

        Config config = new Config();

        String topologyName = HeronStreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
