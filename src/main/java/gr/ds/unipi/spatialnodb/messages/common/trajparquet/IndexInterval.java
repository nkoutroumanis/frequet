package gr.ds.unipi.spatialnodb.messages.common.trajparquet;

import java.io.Serializable;

public class IndexInterval implements Serializable {
    private int start;
    private int end;
    private IndexInterval(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public static IndexInterval newIndexInterval(int start, int end) {
        return new IndexInterval(start, end);
    }
}
