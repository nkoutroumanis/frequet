package tman;/*
 * TShapeTrajectoryRecord.java
 * ============================
 *
 * The Parquet-facing data class for one TShape-indexed trajectory. Modeled
 * directly on the user's own gr.ds.unipi.spatialnodb.messages.common.
 * trajparquet.TrajectorySegment class (same packed-double-array convention
 * for the point list, same min/max longitude/latitude MBR columns), but
 * simpler in one important way:
 *
 * TrajectorySegment exists to hold a *piece* of a trajectory, because that
 * project's Hilbert-grid partitioning can cut one trajectory across several
 * grid cells / directories -- hence the separate "WithIntervalMetadata"
 * variant and the sort-and-glue reassembly logic in
 * Frechet2DQueriesDirectoriesIntervalsDistancesVar1 (group by objectId, sort
 * segments by their interval, verify the intervals are contiguous, then
 * concatenate). TShapeIndex.build() never splits a trajectory: every
 * trajectory is assigned to exactly one enlarged element (findEnlargedElement
 * picks a single (cx, cy, r) anchor for the whole thing). So one
 * TShapeTrajectoryRecord always holds one *complete, unsplit* trajectory --
 * there is no interval/segment metadata and no reassembly step needed on the
 * read side.
 *
 * Columns:
 *   objectId                                    -- trajectory id (String)
 *   longitude, latitude                         -- packed double[] arrays,
 *                                                   one entry per point, same
 *                                                   index i in both arrays is
 *                                                   point i of the trajectory
 *   tshapeValue                                  -- the trajectory's TShape
 *                                                   index key, i.e.
 *                                                   (quadrantCode << (alpha*beta)) | shapeId
 *                                                   from TShapeIndex.build().
 *                                                   Not required for the
 *                                                   pruning strategy used by
 *                                                   TShapeFrechetQuery.java
 *                                                   (which relies on the
 *                                                   MBR columns below and
 *                                                   Parquet filter pushdown
 *                                                   instead), but kept
 *                                                   alongside the points so
 *                                                   the Parquet file is a
 *                                                   complete, self-describing
 *                                                   materialization of the
 *                                                   index -- useful for
 *                                                   debugging, for a future
 *                                                   directory-per-element
 *                                                   partitioning scheme, or
 *                                                   for reconstructing
 *                                                   invertedIndex without
 *                                                   rerunning build().
 *   minLongitude/minLatitude/maxLongitude/maxLatitude
 *                                                -- the trajectory's own
 *                                                   (exact, not the enlarged
 *                                                   element's) bounding box,
 *                                                   used as the Parquet
 *                                                   column-statistics filter
 *                                                   target for Frechet
 *                                                   pruning (see
 *                                                   TShapeIndex.frechetMbrPrune
 *                                                   and TShapeFrechetQuery).
 *
 * No package declaration, by the same "plain .java files, you manage the
 * classpath" convention as TShapeIndex.java/GeolifeExperiment.java -- this
 * sits in the same default package so it can reference the package-visible
 * Point class directly.
 */

import java.io.Serializable;
import java.util.Arrays;

public class TShapeTrajectoryRecord implements Serializable {

    private final String objectId;
    private final Point[] points;
    private final long tshapeValue;

    private final double minLongitude;
    private final double minLatitude;
    private final double maxLongitude;
    private final double maxLatitude;

    public TShapeTrajectoryRecord(
            String objectId,
            Point[] points,
            long tshapeValue,
            double minLongitude,
            double minLatitude,
            double maxLongitude,
            double maxLatitude) {
        this.objectId = objectId;
        this.points = points;
        this.tshapeValue = tshapeValue;
        this.minLongitude = minLongitude;
        this.minLatitude = minLatitude;
        this.maxLongitude = maxLongitude;
        this.maxLatitude = maxLatitude;
    }

    /** Convenience constructor that computes the MBR columns from the points
     *  themselves, for callers that have not already computed the MBR
     *  (TShapeParquetWriter passes it in explicitly instead, since it
     *  already has it on hand from the index). */
    public TShapeTrajectoryRecord(String objectId, Point[] points, long tshapeValue) {
        this(objectId, points, tshapeValue,
                minOf(points, true), minOf(points, false),
                maxOf(points, true), maxOf(points, false));
    }

    private static double minOf(Point[] pts, boolean xAxis) {
        double best = Double.POSITIVE_INFINITY;
        for (Point p : pts) {
            best = Math.min(best, xAxis ? p.x : p.y);
        }
        return best;
    }

    private static double maxOf(Point[] pts, boolean xAxis) {
        double best = Double.NEGATIVE_INFINITY;
        for (Point p : pts) {
            best = Math.max(best, xAxis ? p.x : p.y);
        }
        return best;
    }

    public String getObjectId() {
        return objectId;
    }

    public Point[] getPoints() {
        return points;
    }

    public long getTshapeValue() {
        return tshapeValue;
    }

    public double getMinLongitude() {
        return minLongitude;
    }

    public double getMinLatitude() {
        return minLatitude;
    }

    public double getMaxLongitude() {
        return maxLongitude;
    }

    public double getMaxLatitude() {
        return maxLatitude;
    }

    @Override
    public String toString() {
        return "TShapeTrajectoryRecord{" +
                "objectId='" + objectId + '\'' +
                ", points=" + Arrays.toString(points) +
                ", tshapeValue=" + tshapeValue +
                ", minLongitude=" + minLongitude +
                ", minLatitude=" + minLatitude +
                ", maxLongitude=" + maxLongitude +
                ", maxLatitude=" + maxLatitude +
                '}';
    }
}
