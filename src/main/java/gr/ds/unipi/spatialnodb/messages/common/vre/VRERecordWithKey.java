package gr.ds.unipi.spatialnodb.messages.common.vre;

public class VRERecordWithKey {
    private final long key;
    private final VRERecord record;

    public VRERecordWithKey(long key, VRERecord record) {
        this.key = key;
        this.record = record;
    }

    public VRERecord getRecord() {
        return record;
    }

    public long getKey() {
        return key;
    }
}
