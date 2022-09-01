import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class Generate {
    public static void main(String[] args) throws Exception {
        var path = Paths.get("data.csv");
        long maxSize = 100L * 1000 * 1000 * 1000;
        int col = 10;
        int wordSize = 30;
        
        char[] buf = new char[col * (wordSize + 1)];
        try(var bf = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            long totalSize = 0;
            while (totalSize < maxSize) {
                int size = putLine(buf, col, wordSize);
                bf.write(buf, 0, size);
                totalSize += size;
            }
        }
    }

    private static Random gen = new XorShift();
    private static int putLine(char[] buf, int col, int wordSize) {
        for (int i = 0; i < col; i ++) {
            putWord(buf, i + i * wordSize, wordSize);
            buf[i + (i + 1) * wordSize] = i < col - 1 ? ',' : '\n';
        }
        return col * (wordSize + 1);
    }

    private static void putWord(char[] buf, int offset, int n) {
        for (int i = 0; i < n; i ++) {
            buf[offset + i] = (char)('a' + gen.nextInt(26));
        }
    }

    private static class XorShift extends Random {
        private static final long serialVersionUID = 6806629989739663134L;
        private long x=123456789, y=362436069, z=521288629, w=88675123;
        public XorShift() {super(); x = System.nanoTime();}
        public synchronized void setSeed(long seed) {super.setSeed(seed); x = seed;}
        protected int next(int bits){
            long t=(x^x<<11)&(1L<<32)-1; x=y; y=z; z=w; w=(w^w>>>19^t^t>>>8)&(1L<<32)-1;
            return (int)w>>>32-bits;
        }
    }
}