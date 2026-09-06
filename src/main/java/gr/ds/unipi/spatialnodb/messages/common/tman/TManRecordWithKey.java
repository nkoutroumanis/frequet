package gr.ds.unipi.spatialnodb.messages.common.tman;

public class TManRecordWithKey {

    private final long key;
    private final TManRecord record;

    public TManRecordWithKey(long key, TManRecord record) {
        this.key = key;
        this.record = record;
    }

    public TManRecord getRecord() {
        return record;
    }

    public long getKey() {
        return key;
    }
}
