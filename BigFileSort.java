import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

class BufferedCsvReader implements Closeable {
    private final long bufferSize;
    private final BufferedReader reader;
    private final ArrayDeque<String[]> q = new ArrayDeque<>();

    public BufferedCsvReader(Path path, long bufferSize) throws IOException {
        this.reader = Files.newBufferedReader(path);
        this.bufferSize = bufferSize;
    }

    public String[] peek() {
        readBuf();
        return q.peekFirst();
    }

    public String[] poll() {
        readBuf();
        return q.pollFirst();
    }

    public boolean ready() throws IOException {
        return q.size() > 0 || reader.ready();
    }

    private void readBuf() {
        if (q.isEmpty()) {
            long size = 0;
            try {
                while (size < bufferSize && reader.ready()) {
                    var line = reader.readLine();
                    var key = line.substring(0, line.indexOf(','));
                    size += line.length() + key.length();
                    q.addLast(new String[] { key, line });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

public class BigFileSort {
    private static final long MEMORY_SIZE = 1L * 1000 * 1000 * 1000;
    private static final int MAX_SPLITS = 500;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        sort(Paths.get("data.csv"));

        long end = System.currentTimeMillis();

        System.out.println((end - start) / 1000 + " sec");
    }

    private static void sort(Path path) throws IOException {
        removeTmpFiles(path);
        int cnt = microSort(path);
        macroSort(path, cnt);
    }

    private static void macroSort(Path path, int cnt) throws IOException {
        PriorityQueue<BufferedCsvReader> q = new PriorityQueue<>((o1, o2) -> o1.peek()[0].compareTo(o2.peek()[0]));

        try {
            for (int i = 0; i < cnt; i++) {
                q.add(new BufferedCsvReader(getTmpPath(path, i), MEMORY_SIZE / cnt));
            }
    
            Path outPath = Paths.get(String.format("%s.out.csv", path.toString().toString()));
            try (var writer = Files.newBufferedWriter(outPath)) {
                while (q.size() > 0) {
                    var reader = q.poll();
                    writer.write(reader.poll()[1]);
                    writer.write("\n");
                    if (reader.ready()) {
                        q.add(reader);
                    }
                }
            }
        } finally {
            for (var v : q) {
                v.close();
            }
        }
    }

    private static int microSort(Path path) throws IOException {
        long maxSize = MEMORY_SIZE;

        int cnt = 0;
        List<String[]> list = new ArrayList<>(1000000);

        try (var reader = Files.newBufferedReader(path)) {
            long size = 0;
            while (reader.ready()) {
                var line = reader.readLine();
                var key = line.substring(0, line.indexOf(','));

                list.add(new String[] { key, line });
                size += line.length() + key.length();

                if (size >= maxSize || !reader.ready()) {
                    System.out.println(cnt + "th File created.");
                    Collections.sort(list, (o1, o2) -> o1[0].compareTo(o2[0]));
                    try (var writer = Files.newBufferedWriter(getTmpPath(path, cnt), StandardCharsets.UTF_8)) {
                        for (var v : list) {
                            writer.write(v[1]);
                            writer.write("\n");
                        }
                    }
                    list.clear();
                    size = 0;
                    cnt++;
                    if (cnt >= MAX_SPLITS) {
                        throw new RuntimeException("Too many files");
                    }
                }
            }
        }
        return cnt;
    }

    private static Path getTmpPath(Path path, int i) {
        return Paths.get(String.format("/tmp/%s.%08d", path.getFileName().toString().toString(), i));
    }

    private static void removeTmpFiles(Path path) throws IOException {
        for (int i = 0;; i++) {
            Path tmpPath = getTmpPath(path, i);
            if (Files.exists(tmpPath)) {
                Files.delete(tmpPath);
            } else {
                break;
            }
        }
    }
}
