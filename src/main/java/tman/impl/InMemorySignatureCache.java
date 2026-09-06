package tman.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class InMemorySignatureCache
        implements CachedShapeQuery.SignatureCache, FrechetCachedQuery.SignatureCache {

    private final XZConfig cfg;
    private final Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
    // cached immutable arrays, invalidated on write
    private final Map<Long, long[]> arrCache = new HashMap<Long, long[]>();

    public InMemorySignatureCache(XZConfig cfg) {
        this.cfg = cfg;
    }

    public static InMemorySignatureCache fromMap(
            Map<Long, ? extends Set<Long>> source, XZConfig cfg, boolean share) {
        InMemorySignatureCache c = new InMemorySignatureCache(cfg);
        for (Map.Entry<Long, ? extends Set<Long>> e : source.entrySet()) {
            Set<Long> sigs = e.getValue();
            if (sigs == null || sigs.isEmpty()) {
                continue;
            }
            if (share && sigs instanceof HashSet) {
                @SuppressWarnings("unchecked")
                Set<Long> adopted = (Set<Long>) sigs;
                c.map.put(e.getKey(), adopted);
            } else {
                c.map.put(e.getKey(), new HashSet<Long>(sigs));
            }
        }
        return c;
    }

    public static InMemorySignatureCache fromMap(
            Map<Long, ? extends Set<Long>> source, XZConfig cfg) {
        return fromMap(source, cfg, false);
    }

    // ------------------------------------------------------------- write path

    public void record(long code, long s) {
        Set<Long> set = map.get(code);
        if (set == null) {
            set = new HashSet<Long>();
            map.put(code, set);
        }
        if (set.add(s)) {
            arrCache.remove(code);          // invalidate memoized array
        }
    }

    public void addPackedKey(long tshapeKey) {
        long s = tshapeKey & (cfg.shapeSlots() - 1);
        long code = tshapeKey >>> cfg.shapeBits();
        record(code, s);
    }

    // -------------------------------------------------------------- read path

    @Override
    public long[] signaturesForCell(long quadrantCode) {
        long[] cached = arrCache.get(quadrantCode);
        if (cached != null) {
            return cached;
        }
        Set<Long> set = map.get(quadrantCode);
        if (set == null) {
            return null;
        }
        long[] arr = new long[set.size()];
        int i = 0;
        for (long s : set) {
            arr[i++] = s;
        }
        java.util.Arrays.sort(arr);
        arrCache.put(quadrantCode, arr);
        return arr;
    }

    // ------------------------------------------------------------- bulk build

    public static InMemorySignatureCache fromKeys(long[] tshapeKeys, XZConfig cfg) {
        InMemorySignatureCache c = new InMemorySignatureCache(cfg);
        for (long k : tshapeKeys) {
            c.addPackedKey(k);
        }
        return c;
    }
}