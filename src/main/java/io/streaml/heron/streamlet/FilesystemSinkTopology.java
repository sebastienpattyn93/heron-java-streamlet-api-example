package io.streaml.heron.streamlet;

import com.twitter.heron.dsl.*;

import java.io.*;

public class FilesystemSinkTopology {
    private static class FilesystemSink<T> implements Sink<T> {
        private PrintWriter writer;

        public void setup(Context context) {
            try {
                File f = File.createTempFile("test", ".tmp");
                this.writer = new PrintWriter(new FileWriter(f));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void put(T element) {
            writer.append(element.toString());
            writer.flush();
            writer.close();
        }

        public void cleanup() {}
    }

    public static void main(String[] args) throws IOException {
        Builder builder = Builder.createBuilder();

        builder.newSource(() -> "word")
                .toSink(new FilesystemSink<>());

        Config config = new Config();

        new Runner().run(args[0], config, Builder.createBuilder());
    }
}
