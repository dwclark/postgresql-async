package db.postgresql.async.buffers;

public class Chunk {

    private final int index;
    private int total;

    public Chunk(final int index) {
        this.index = index;
        this.total = total;
    }

    public void plus(final int val) {
        total += val;
    }
}
