package tman;/*
 * TShapeFrechetQuery.java
 * ========================
 *
 * Threshold discrete-Frechet similarity query over TShape-indexed
 * trajectories stored as Parquet, modeled on the user's own Spark-based
 * query class:
 *
 *   gr.ds.unipi.spatialnodb.queries.frequet
 *     .Frechet2DQueriesDirectoriesIntervalsDistancesVar1
 *
 * What's kept the same as that reference class:
 *   - Spark (JavaSparkContext over a SparkSession), reading Parquet through
 *     the classic ParquetInputFormat + a custom ReadSupport (here,
 *     TShapeTrajectoryRecordReadSupport), exactly like
 *     ParquetInputFormat.setReadSupportClass(job, ...) /
 *     jsc.newAPIHadoopFile(path, ParquetInputFormat.class, Void.class,
 *     RecordType.class, job.getConfiguration()) there.
 *   - The overall shape of the pruning pipeline: expand the query
 *     trajectory's MBR by epsilon, use that as a Parquet FilterPredicate
 *     over min/max longitude/latitude columns so row groups that cannot
 *     possibly match are skipped before any point data is read, then apply
 *     cheaper-than-exact local filtering before the final expensive exact
 *     distance computation.
 *
 * What's different, and why:
 *   - Reference class's FilterPredicate is combined with a *separate*,
 *     coarser Hilbert-grid directory-skip pass (only directories whose
 *     Hilbert cell is within epsilon of the query trajectory are even
 *     listed as input paths). This class relies on Parquet's own row-group
 *     statistics and FilterPredicate evaluation alone ("pruning on top of
 *     Parquet" via column stats, which is the pruning strategy requested)
 *     -- TShape does not need a separate directory layout because a
 *     trajectory's exact MBR is already stored as plain DOUBLE columns on
 *     every record, which parquet-column tracks min/max statistics for
 *     per row group automatically.
 *   - The reference class's FilterPredicate condition is exactly the
 *     Frechet MBR-containment test (candidate MBR contained in the
 *     epsilon-expanded query MBR); this class reuses the identical
 *     condition, just expressed as a FilterPredicate instead of a Java
 *     method call, so it is provably the same rule as
 *     TShapeIndex.frechetMbrPrune() -- see that method's Javadoc for the
 *     Hausdorff-distance soundness argument.
 *   - No segment/interval reassembly (groupBy + sort-and-glue): as
 *     explained in TShapeTrajectoryRecord's class comment, TShape never
 *     splits a trajectory across records, so each surviving Parquet row is
 *     already a complete trajectory.
 *   - The local filter (TShapeIndex.frechetLocalFilter) and the exact
 *     refinement (TShapeIndex.discreteFrechet) are the exact same static
 *     methods used by the pure in-memory index's frechetRangeQuery(), not
 *     reimplemented here -- so the in-memory path and this Parquet/Spark
 *     path are guaranteed to agree (both were validated together; see the
 *     project notes for the random-trajectory soundness stress test).
 *   - Per the explicit requirement "for a trajectory query I want the
 *     points of the matched trajectory to be returned": every match keeps
 *     the record's full Point[] (via FrechetMatch, the very same result
 *     type frechetRangeQuery() returns), not just an id.
 *
 * Build / run
 * -----------
 * Plain .java, no build file (per project convention) -- compile and run
 * this against the exact same classpath you already use for your frequet
 * project's Spark jobs (this class needs the same Spark, Hadoop and Parquet
 * jars as Frechet2DQueriesDirectoriesIntervalsDistancesVar1, since it is
 * structured the same way):
 *
 *     javac -cp "<frequet-classpath>" TShapeIndex.java TShapeTrajectoryRecord.java \
 *         TShapeTrajectoryRecordWriteSupport.java TShapeTrajectoryRecordReadSupport.java \
 *         TShapeTrajectoryRecordMaterializer.java TShapeParquetWriter.java TShapeFrechetQuery.java
 *
 *     java -cp ".:<frequet-classpath>" TShapeFrechetQuery \
 *         --parquet-path /path/to/tshape.parquet \
 *         --query-file query_trajectory.csv \
 *         --epsilon 0.01
 *
 * query_trajectory.csv is a plain text file, one point per line, "lon,lat".
 */

import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;

public class TShapeFrechetQuery {

    /** All parsed CLI arguments, gathered in one place (same pattern as
     *  GeolifeExperiment.Args). */
    static final class Args {
        String parquetPath;
        String queryFile;
        double epsilon;
        String outputPath = null;
    }

    public static void main(String[] argv) throws IOException {
        Args args = parseArgs(argv);
        if (args == null) {
            return;
        }

        Point[] queryTrajectory = loadQueryTrajectory(args.queryFile);
        Bounds queryMbr = computeMbr(queryTrajectory);
        // The single FilterPredicate below encodes the exact same
        // MBR-containment rule as TShapeIndex.frechetMbrPrune(candidateMbr,
        // expandedQueryMbr): a candidate's own (minLongitude, minLatitude,
        // maxLongitude, maxLatitude) columns must fall entirely inside the
        // query trajectory's MBR expanded by epsilon in every direction.
        // Parquet evaluates this against each row group's column
        // statistics first (skipping the row group entirely if it cannot
        // possibly satisfy it) and then per-row as records are read, so no
        // point data is touched for anything that is provably out of range.
        double expMinLon = queryMbr.xmin - args.epsilon;
        double expMinLat = queryMbr.ymin - args.epsilon;
        double expMaxLon = queryMbr.xmax + args.epsilon;
        double expMaxLat = queryMbr.ymax + args.epsilon;

        FilterPredicate lonRange = and(
                gtEq(doubleColumn("minLongitude"), expMinLon),
                ltEq(doubleColumn("maxLongitude"), expMaxLon));
        FilterPredicate latRange = and(
                gtEq(doubleColumn("minLatitude"), expMinLat),
                ltEq(doubleColumn("maxLatitude"), expMaxLat));
        FilterPredicate predicate = and(lonRange, latRange);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TShape Frechet Similarity Query");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory", "4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        try {
            Job job = Job.getInstance();
            ParquetInputFormat.setReadSupportClass(job, TShapeTrajectoryRecordReadSupport.class);
            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), predicate);

            @SuppressWarnings("unchecked")
            JavaPairRDD<Void, TShapeTrajectoryRecord> rangePruned =
                    (JavaPairRDD<Void, TShapeTrajectoryRecord>) jsc.newAPIHadoopFile(
                            args.parquetPath,
                            ParquetInputFormat.class,
                            Void.class,
                            TShapeTrajectoryRecord.class,
                            job.getConfiguration());

            // Broadcast the (typically tiny) query trajectory and epsilon
            // once instead of shipping them with every closure.
            Broadcast<Point[]> broadcastQuery = jsc.broadcast(queryTrajectory);
            double epsilon = args.epsilon;

            long t0 = System.currentTimeMillis();

            JavaRDD<FrechetMatch> matches = rangePruned
                    .map(Tuple2::_2)
                    // Local filter: cheap O(n*m) nearest-point scan, no DP
                    // table -- see TShapeIndex.frechetLocalFilter's Javadoc.
                    .filter(rec -> TShapeIndex.frechetLocalFilter(rec.getPoints(), broadcastQuery.value(), epsilon))
                    // Exact refinement: the real discrete Frechet distance,
                    // run only on whatever survived the two prune stages.
                    .map(rec -> new FrechetMatch(
                            rec.getObjectId(),
                            Arrays.asList(rec.getPoints()),
                            TShapeIndex.discreteFrechet(rec.getPoints(), broadcastQuery.value())))
                    .filter(m -> m.distance <= epsilon);

            List<FrechetMatch> results = matches.collect();
            results.sort((a, b) -> Double.compare(a.distance, b.distance));

            long elapsedMs = System.currentTimeMillis() - t0;

            System.out.println("Query trajectory points: " + queryTrajectory.length);
            System.out.println("Epsilon: " + args.epsilon);
            System.out.println("Matches: " + results.size() + " (in " + elapsedMs + " ms)");
            for (FrechetMatch match : results) {
                System.out.println(formatMatch(match));
            }

            if (args.outputPath != null) {
                writeResults(results, args.outputPath);
                System.out.println("Wrote results to " + args.outputPath);
            }
        } finally {
            sparkSession.close();
        }
    }

    /** Human-readable one-line rendering of a match: id, distance, and every
     *  point of the matched trajectory (the explicit requirement this class
     *  exists to satisfy -- callers get the geometry, not just an id). */
    private static String formatMatch(FrechetMatch match) {
        StringBuilder sb = new StringBuilder();
        sb.append(match.trajectoryId).append('\t').append(match.distance).append('\t');
        for (int i = 0; i < match.points.size(); i++) {
            if (i > 0) {
                sb.append(';');
            }
            Point p = match.points.get(i);
            sb.append(p.x).append(',').append(p.y);
        }
        return sb.toString();
    }

    private static void writeResults(List<FrechetMatch> results, String outputPath) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
            bw.write("trajectoryId\tdistance\tpoints(lon,lat;lon,lat;...)");
            bw.newLine();
            for (FrechetMatch match : results) {
                bw.write(formatMatch(match));
                bw.newLine();
            }
        }
    }

    /** Read a query trajectory from a plain text file, one "lon,lat" point
     *  per line. Deliberately much simpler than the reference class's
     *  packed "lon,lat,timestamp;..." single-line format, since TShape (and
     *  discrete Frechet distance as implemented here) has no temporal
     *  dimension. */
    private static Point[] loadQueryTrajectory(String path) throws IOException {
        List<Point> points = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 2) {
                    continue;
                }
                double lon = Double.parseDouble(parts[0].trim());
                double lat = Double.parseDouble(parts[1].trim());
                points.add(new Point(lon, lat));
            }
        }
        if (points.isEmpty()) {
            throw new IllegalArgumentException("Query file " + path + " contained no points");
        }
        return points.toArray(new Point[0]);
    }

    private static Bounds computeMbr(Point[] points) {
        double xmin = Double.POSITIVE_INFINITY;
        double ymin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY;
        double ymax = Double.NEGATIVE_INFINITY;
        for (Point p : points) {
            xmin = Math.min(xmin, p.x);
            ymin = Math.min(ymin, p.y);
            xmax = Math.max(xmax, p.x);
            ymax = Math.max(ymax, p.y);
        }
        return new Bounds(xmin, ymin, xmax, ymax);
    }

    private static void printHelp() {
        System.out.println("Usage: java TShapeFrechetQuery --parquet-path <path> --query-file <path> --epsilon <value> [--output <path>]");
        System.out.println();
        System.out.println("  --parquet-path <path>  Directory or file of TShapeTrajectoryRecord Parquet data");
        System.out.println("                          (written by TShapeParquetWriter).");
        System.out.println("  --query-file <path>     Text file, one \"lon,lat\" point per line.");
        System.out.println("  --epsilon <value>       Discrete Frechet distance threshold, in the same");
        System.out.println("                          coordinate units as the points (e.g. degrees).");
        System.out.println("  --output <path>         Optional: also write results to this tab-separated file.");
    }

    private static Args parseArgs(String[] argv) {
        Args args = new Args();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            switch (a) {
                case "--help":
                case "-h":
                    printHelp();
                    return null;
                case "--parquet-path":
                    args.parquetPath = argv[++i];
                    break;
                case "--query-file":
                    args.queryFile = argv[++i];
                    break;
                case "--epsilon":
                    args.epsilon = Double.parseDouble(argv[++i]);
                    break;
                case "--output":
                    args.outputPath = argv[++i];
                    break;
                default:
                    System.out.println("Unknown argument: " + a);
                    printHelp();
                    return null;
            }
        }
        if (args.parquetPath == null || args.queryFile == null) {
            System.out.println("--parquet-path and --query-file are required.");
            printHelp();
            return null;
        }
        return args;
    }
}
