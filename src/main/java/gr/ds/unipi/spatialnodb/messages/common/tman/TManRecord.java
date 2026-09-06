package gr.ds.unipi.spatialnodb.messages.common.tman;

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import tman.impl.DPFeatureExtractor;

import java.io.Serializable;
import java.util.Arrays;

public class TManRecord implements Serializable {

    private final String objectId;
    private final SpatialPoint[] spatialPoints;
    private final int[] dpPointIndexes;
    private final DPFeatureExtractor.BoundingBox[] dpMbrs;

    public TManRecord(String objectId, SpatialPoint[] spatialPoints, int[] dpPointIndexes, DPFeatureExtractor.BoundingBox[] dpMbrs) {
        this.objectId = objectId;
        this.spatialPoints = spatialPoints;
        this.dpPointIndexes = dpPointIndexes;
        this.dpMbrs = dpMbrs;
    }

    public String getObjectId() {
        return objectId;
    }

    public SpatialPoint[] getSpatialPoints() {
        return spatialPoints;
    }

    @Override
    public String toString() {
        return "TManRecord{" +
                "objectId='" + objectId + '\'' +
                ", spatioTemporalPoints=" + Arrays.toString(spatialPoints) +
                '}';
    }

    public int[] getDpPointIndexes() {
        return dpPointIndexes;
    }

    public DPFeatureExtractor.BoundingBox[] getDpMbrs() {
        return dpMbrs;
    }
}
