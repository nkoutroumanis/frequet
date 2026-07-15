package tman;/*
 * TShapeParquetWriter.java
 * =========================
 *
 * Writes a built TShapeIndex out to a single Parquet file of
 * TShapeTrajectoryRecord rows -- this is the piece that answers "I need to
 * store the TShapeIndex records to Parquet files".
 *
 * Deliberately NOT Spark-based, unlike TShapeFrechetQuery.java (the query
 * side). Building a TShapeIndex happens once, in one JVM, from an
 * already-in-memory Map<String, List<Point>> (see TShapeIndex.build()) --
 * there is no distributed input to parallelize over, so a plain
 * single-process org.apache.parquet.hadoop.ParquetWriter is simpler and
 * avoids spinning up Spark just to serialize a HashMap. The *query* side
 * needs Spark because it scans many trajectories across (potentially many)
 * Parquet files/row-groups; the *write* side does not.
 *
 * ParquetWriter has no public constructor -- parquet-mr's idiom is a
 * per-record-type Builder subclass that supplies the WriteSupport. This is
 * the standard, minimal version of that idiom (compare e.g. parquet-avro's
 * AvroParquetWriter.Builder, or parquet-mr's own ExampleParquetWriter).
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TShapeParquetWriter {

    /** Builder wiring TShapeTrajectoryRecordWriteSupport into ParquetWriter's
     *  generic builder machinery. Package-private nested static class kept
     *  here rather than in its own file since nothing else needs it. */
    private static final class Builder extends ParquetWriter.Builder<TShapeTrajectoryRecord, Builder> {
        private Builder(Path path) {
            super(path);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<TShapeTrajectoryRecord> getWriteSupport(Configuration conf) {
            return new TShapeTrajectoryRecordWriteSupport();
        }
    }

    /** Open a writer at the given path (local path or hdfs://... URI) with
     *  Snappy compression -- the same codec used elsewhere for the columnar
     *  double-array/binary columns TShapeTrajectoryRecord shares with
     *  TrajectorySegment. Caller is responsible for closing it (or use
     *  try-with-resources; ParquetWriter implements Closeable). */
    public static ParquetWriter<TShapeTrajectoryRecord> open(String path) throws IOException {
        return new Builder(new Path(path))
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    /**
     * Materialize every trajectory in a built TShapeIndex to a Parquet file.
     *
     * Reads directly from the index's public primaryTable (trajectoryId ->
     * points) and records (trajectoryId -> TShapeRecord, for tshapeValue) --
     * both are already public fields on TShapeIndex, so no changes to
     * TShapeIndex.java were needed to support this. The per-trajectory MBR
     * stored in each Parquet row is recomputed here from the raw points
     * (deliberately the trajectory's own exact MBR, not the coarser enlarged
     * element's footprint -- see TShapeTrajectoryRecord's class comment for
     * why the exact MBR matters for Frechet pruning).
     *
     * @param index      a TShapeIndex that has already had build() called.
     * @param outputPath local path or hdfs://... URI to write to. Must not
     *                   already exist (ParquetWriter, like Hadoop's other
     *                   output formats, will not silently overwrite).
     */
    public static void writeIndex(TShapeIndex index, String outputPath) throws IOException {
        try (ParquetWriter<TShapeTrajectoryRecord> writer = open(outputPath)) {
            for (Map.Entry<String, List<Point>> entry : index.primaryTable.entrySet()) {
                String trajectoryId = entry.getKey();
                List<Point> trajectory = entry.getValue();
                if (trajectory.isEmpty()) {
                    continue;
                }

                TShapeRecord meta = index.records.get(trajectoryId);
                long tshapeValue = (meta != null) ? meta.tshapeValue : 0L;

                double xmin = Double.POSITIVE_INFINITY;
                double ymin = Double.POSITIVE_INFINITY;
                double xmax = Double.NEGATIVE_INFINITY;
                double ymax = Double.NEGATIVE_INFINITY;
                for (Point p : trajectory) {
                    xmin = Math.min(xmin, p.x);
                    ymin = Math.min(ymin, p.y);
                    xmax = Math.max(xmax, p.x);
                    ymax = Math.max(ymax, p.y);
                }

                writer.write(new TShapeTrajectoryRecord(
                        trajectoryId,
                        trajectory.toArray(new Point[0]),
                        tshapeValue,
                        xmin, ymin, xmax, ymax));
            }
        }
    }

    /** Small standalone smoke test mirroring TShapeIndex.main()'s synthetic
     *  trajectories -- writes them to a Parquet file passed as args[0], so
     *  it can be immediately fed to TShapeFrechetQuery for an end-to-end
     *  check on your own machine (this sandbox has no Parquet/Hadoop jars
     *  to actually run this against -- see the project notes). */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java TShapeParquetWriter <output-parquet-path>");
            return;
        }

        Map<String, List<Point>> trajectories = new java.util.LinkedHashMap<>();
        trajectories.put("t1", java.util.Arrays.asList(new Point(0.0, 0.0), new Point(1.0, 1.0), new Point(2.0, 1.0)));
        trajectories.put("t2", java.util.Arrays.asList(new Point(5.0, 5.0), new Point(6.0, 5.0), new Point(7.0, 6.0)));
        trajectories.put("t3", java.util.Arrays.asList(new Point(0.0, 5.0), new Point(1.0, 4.0), new Point(2.0, 3.0)));
        trajectories.put("t4", java.util.Arrays.asList(new Point(1.0, 0.0), new Point(1.2, 0.5), new Point(1.7, 1.5)));

        TShapeIndex index = new TShapeIndex(3, 3, 8, null, 16, 16, 2_000_000L, 7L);
        index.build(trajectories);

        writeIndex(index, args[0]);
        System.out.println("Wrote " + trajectories.size() + " trajectories to " + args[0]);
    }
}
