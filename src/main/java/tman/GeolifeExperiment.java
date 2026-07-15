package tman;/*
 * GeolifeExperiment.java
 * =======================
 *
 * Java port of experiment_geolife.py: builds a TShapeIndex over a (bounded)
 * sample of the GeoLife Trajectories 1.3 dataset and runs a spatial range
 * query benchmark loosely modeled after the TMan paper's evaluation
 * methodology (Section VI). See TShapeIndex.java for the index itself; this
 * file only adds dataset loading, query sampling, a brute-force ground
 * truth, and reporting on top of it.
 *
 * No package, no build system -- compile together with TShapeIndex.java:
 *
 *     javac TShapeIndex.java GeolifeExperiment.java
 *     java GeolifeExperiment --help
 *     java GeolifeExperiment --geolife-path "/path/to/Geolife Trajectories 1.3/Data"
 *
 * The defaults are tuned to finish in well under a minute, the same as the
 * Python version's defaults -- see printUsageNotes() / --help. What
 * dominates run time, in order:
 *
 *   1. Brute-force ground-truth verification (bruteForceQuery): a linear
 *      pass over *every point in the loaded dataset*, run at least once per
 *      window size regardless of --verify-every (the first query of every
 *      window is always checked). --no-verify skips this entirely.
 *   2. --max-trajectories and --max-points-per-trajectory, which bound how
 *      much data there is to rasterize (build) and brute-force-scan
 *      (verify).
 *   3. --num-queries * (number of --window-sizes-m values).
 *
 * Outputs
 * -------
 * Always prints a human-readable report to stdout. Unless --no-save is
 * given, also writes (overwriting any previous run):
 *
 *     <output-dir>/geolife_benchmark.csv           one row per query
 *     <output-dir>/geolife_benchmark_summary.json  aggregated per-window stats
 *     <output-dir>/geolife_benchmark.png           charts (pure java.awt --
 *                                                   no plotting library
 *                                                   needed, unlike the
 *                                                   Python version's
 *                                                   matplotlib dependency)
 *
 * --output-dir defaults to "reports" resolved against the current working
 * directory (Java has no exact equivalent of Python's __file__-relative
 * default, so unlike the Python script this is CWD-relative -- pass
 * --output-dir explicitly if you run this from somewhere other than the
 * source directory).
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;

public class GeolifeExperiment {

    private static final double METERS_PER_DEGREE_LAT = 111_320.0;

    // =====================================================================
    // Dataset loading
    // =====================================================================

    /**
     * Load GeoLife .plt trajectory files into {trajectoryId: [(lon, lat), ...]}.
     *
     * GeoLife's .plt format has a fixed 6-line header per file; each
     * remaining line is "lat,lon,0,altitude,days,date,time". We keep only
     * (lon, lat), matching this port's (x, y) = (longitude, latitude)
     * convention.
     *
     * maxTrajectories stops walking the directory tree early once enough
     * trajectories have been collected, so this never reads the whole
     * dataset -- only as many files as needed to reach the requested sample
     * size.
     *
     * maxPointsPerTrajectory optionally truncates each trajectory to its
     * first N points (null/<=0 means no truncation). GeoLife logs can run
     * for many hours (thousands of points per file); both the TShape
     * rasterizer and, more importantly, the brute-force ground-truth check
     * are O(points), so a handful of very long trajectories can dominate
     * the whole benchmark's run time.
     */
    static LinkedHashMap<String, List<Point>> loadGeolife(
            String rootDir, Integer maxTrajectories, int minPoints, Integer maxPointsPerTrajectory) throws IOException {
        LinkedHashMap<String, List<Point>> trajectories = new LinkedHashMap<>();
        int[] count = {0};

        List<File> pltFiles = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(new File(rootDir).toPath())) {
            walk.filter(p -> p.toString().endsWith(".plt")).forEach(p -> pltFiles.add(p.toFile()));
        }
        // Sort for reproducibility across platforms/filesystems (Files.walk
        // order is otherwise filesystem-dependent).
        pltFiles.sort(Comparator.comparing(File::getPath));

        for (File file : pltFiles) {
            if (maxTrajectories != null && count[0] >= maxTrajectories) {
                break;
            }

            List<Point> points = new ArrayList<>();
            List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
            for (int i = 6; i < lines.size(); i++) { // skip GeoLife's fixed 6-line header
                String line = lines.get(i).trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 2) {
                    continue;
                }
                try {
                    double lat = Double.parseDouble(parts[0]);
                    double lon = Double.parseDouble(parts[1]);
                    points.add(new Point(lon, lat));
                } catch (NumberFormatException ignored) {
                    continue;
                }
                if (maxPointsPerTrajectory != null && maxPointsPerTrajectory > 0 && points.size() >= maxPointsPerTrajectory) {
                    break;
                }
            }

            if (points.size() >= minPoints) {
                String tid = "traj_" + count[0];
                trajectories.put(tid, points);
                count[0]++;
            }
        }

        return trajectories;
    }

    static void printDatasetStats(Map<String, List<Point>> trajectories) {
        int numTraj = trajectories.size();
        long numPoints = 0;
        double xmin = Double.POSITIVE_INFINITY;
        double ymin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY;
        double ymax = Double.NEGATIVE_INFINITY;

        for (List<Point> traj : trajectories.values()) {
            numPoints += traj.size();
            for (Point p : traj) {
                xmin = Math.min(xmin, p.x);
                ymin = Math.min(ymin, p.y);
                xmax = Math.max(xmax, p.x);
                ymax = Math.max(ymax, p.y);
            }
        }

        System.out.println("Dataset statistics");
        System.out.println("-------------------");
        System.out.println("Trajectories: " + numTraj);
        System.out.println("Points: " + numPoints);
        System.out.printf("Avg points / trajectory: %.2f%n", numTraj > 0 ? (double) numPoints / numTraj : 0.0);
        System.out.printf(
                "Bounds (lon_min, lat_min, lon_max, lat_max): (%.5f, %.5f, %.5f, %.5f)%n", xmin, ymin, xmax, ymax);
    }

    // =====================================================================
    // Geo helpers: relate query window sizes in meters to (lon, lat) degrees
    // =====================================================================

    /**
     * Approximate conversion of a distance in meters to (lonDegrees,
     * latDegrees) at a given reference latitude.
     *
     * Degrees of latitude are worth a roughly constant ~111.32 km
     * everywhere; degrees of longitude shrink by cos(latitude) as you move
     * away from the equator. GeoLife is centered on Beijing (~40 deg N), so
     * a "500m" query window is noticeably wider in degrees of longitude
     * than of latitude. Accurate to well under 1% for windows up to a few
     * km, which is the paper's tested range (100m-2500m).
     *
     * @return {lonDegrees, latDegrees}
     */
    static double[] metersToDegrees(double meters, double refLatDeg) {
        double latDeg = meters / METERS_PER_DEGREE_LAT;
        double lonDeg = meters / (METERS_PER_DEGREE_LAT * Math.cos(Math.toRadians(refLatDeg)));
        return new double[] {lonDeg, latDeg};
    }

    // =====================================================================
    // Query generation
    // =====================================================================

    /**
     * For each requested window size (in meters), generate numQueries random
     * query rectangles of that size, each centered on a randomly chosen
     * *real* trajectory point.
     *
     * Centering on real points (rather than sampling uniformly over the
     * whole bounding box) mirrors the paper's setting of drawing query
     * windows "within the spatio-temporal range" of the dataset: GeoLife
     * trajectories cluster tightly around central Beijing, so a query
     * centered on a uniformly random point in the bounding box would very
     * often land in an empty area and trivially return zero results.
     */
    static LinkedHashMap<Double, List<Bounds>> sampleQueryWindows(
            Map<String, List<Point>> trajectories, List<Double> windowSizesM, int numQueries, Random rng) {
        List<Point> allPoints = new ArrayList<>();
        for (List<Point> traj : trajectories.values()) {
            allPoints.addAll(traj);
        }
        if (allPoints.isEmpty()) {
            throw new IllegalArgumentException("No points available to center query windows on");
        }

        double refLat = medianLat(allPoints);

        LinkedHashMap<Double, List<Bounds>> windows = new LinkedHashMap<>();
        for (double sizeM : windowSizesM) {
            double[] span = metersToDegrees(sizeM, refLat);
            double lonHalf = span[0] / 2.0;
            double latHalf = span[1] / 2.0;

            List<Bounds> boxes = new ArrayList<>();
            for (int i = 0; i < numQueries; i++) {
                Point center = allPoints.get(rng.nextInt(allPoints.size()));
                boxes.add(new Bounds(center.x - lonHalf, center.y - latHalf, center.x + lonHalf, center.y + latHalf));
            }
            windows.put(sizeM, boxes);
        }
        return windows;
    }

    private static double medianLat(List<Point> points) {
        List<Double> lats = new ArrayList<>();
        for (Point p : points) {
            lats.add(p.y);
        }
        java.util.Collections.sort(lats);
        int n = lats.size();
        return n % 2 == 1 ? lats.get(n / 2) : (lats.get(n / 2 - 1) + lats.get(n / 2)) / 2.0;
    }

    // =====================================================================
    // Brute-force reference (ground truth + "no index" baseline timing)
    // =====================================================================

    /**
     * Independent, index-free reference implementation of an exact spatial
     * range query: check every segment of every trajectory directly against
     * the query rectangle.
     *
     * This deliberately does not use TShapeIndex at all (beyond its static,
     * index-independent segmentIntersectsRect geometry predicate), so it can
     * serve as ground truth to catch bugs in queryCandidates()/
     * exactRangeQuery(), and as a "no index" timing baseline.
     */
    static Set<String> bruteForceQuery(Map<String, List<Point>> trajectories, Bounds queryRect) {
        Set<String> matches = new java.util.HashSet<>();
        for (Map.Entry<String, List<Point>> entry : trajectories.entrySet()) {
            List<Point> traj = entry.getValue();
            for (int i = 0; i < traj.size() - 1; i++) {
                if (TShapeIndex.segmentIntersectsRect(traj.get(i), traj.get(i + 1), queryRect)) {
                    matches.add(entry.getKey());
                    break;
                }
            }
        }
        return matches;
    }

    // =====================================================================
    // Benchmark runner
    // =====================================================================

    /** One row of raw per-query benchmark data (used for the CSV export). */
    static final class QueryRow {
        final double windowM;
        final int queryId;
        final int numCandidates;
        final int numExact;
        final double candidateTimeS;
        final double exactTimeS;
        final double totalTimeS;
        final boolean verified;
        final Boolean correct; // null if not verified
        final Double bruteForceTimeS; // null if not verified

        QueryRow(
                double windowM,
                int queryId,
                int numCandidates,
                int numExact,
                double candidateTimeS,
                double exactTimeS,
                double totalTimeS,
                boolean verified,
                Boolean correct,
                Double bruteForceTimeS) {
            this.windowM = windowM;
            this.queryId = queryId;
            this.numCandidates = numCandidates;
            this.numExact = numExact;
            this.candidateTimeS = candidateTimeS;
            this.exactTimeS = exactTimeS;
            this.totalTimeS = totalTimeS;
            this.verified = verified;
            this.correct = correct;
            this.bruteForceTimeS = bruteForceTimeS;
        }
    }

    /**
     * Run the full benchmark: for every window size, generate numQueries
     * random query rectangles (sampleQueryWindows), time
     * queryCandidates()/exactRangeQuery() via
     * TShapeIndex.benchmarkSpatialQueries(), and -- every verifyEvery-th
     * query, if verify is true -- additionally run bruteForceQuery() to (a)
     * confirm exactRangeQuery()'s result set exactly matches the ground
     * truth, and (b) record how long the "no index" baseline takes.
     *
     * Set verify=false to skip brute-force checking entirely (fastest
     * option).
     */
    static List<QueryRow> runQueryBenchmark(
            TShapeIndex idx,
            Map<String, List<Point>> trajectories,
            List<Double> windowSizesM,
            int numQueries,
            Random rng,
            int verifyEvery,
            boolean verify) {
        LinkedHashMap<Double, List<Bounds>> windows = sampleQueryWindows(trajectories, windowSizesM, numQueries, rng);
        List<QueryRow> rows = new ArrayList<>();

        for (Map.Entry<Double, List<Bounds>> windowEntry : windows.entrySet()) {
            double windowM = windowEntry.getKey();
            List<Bounds> rects = windowEntry.getValue();
            List<QueryTiming> timings = idx.benchmarkSpatialQueries(rects);

            for (int qi = 0; qi < rects.size(); qi++) {
                Bounds rect = rects.get(qi);
                QueryTiming timing = timings.get(qi);

                boolean verified = verify && (qi % verifyEvery == 0);
                Boolean correct = null;
                Double bfTime = null;

                if (verified) {
                    Set<String> exactResult = new java.util.HashSet<>(idx.exactRangeQuery(rect));
                    long t0 = System.nanoTime();
                    Set<String> truth = bruteForceQuery(trajectories, rect);
                    bfTime = (System.nanoTime() - t0) / 1_000_000_000.0;
                    correct = exactResult.equals(truth);
                }

                rows.add(
                        new QueryRow(
                                windowM,
                                qi,
                                timing.numCandidates,
                                timing.numExact,
                                timing.candidateTimeS,
                                timing.exactTimeS,
                                timing.totalTimeS,
                                verified,
                                correct,
                                bfTime));
            }
        }

        return rows;
    }

    // =====================================================================
    // Reporting
    // =====================================================================

    private static double percentile(List<Double> values, double pct) {
        if (values.isEmpty()) {
            return 0.0;
        }
        List<Double> ordered = new ArrayList<>(values);
        java.util.Collections.sort(ordered);
        int idx = (int) Math.ceil(pct / 100.0 * ordered.size()) - 1;
        idx = Math.min(ordered.size() - 1, Math.max(0, idx));
        return ordered.get(idx);
    }

    /** Aggregated per-window statistics, mirroring the dict Python's
     *  summarize() produces for each window size. */
    static final class WindowSummary {
        double windowM;
        int numQueries;
        double p50TotalTimeS;
        double p90TotalTimeS;
        double p99TotalTimeS;
        double avgCandidates;
        double avgExactMatches;
        double avgPrecision;
        int numVerified;
        int numCorrect;
        Double avgBruteForceTimeS; // null if nothing verified
        Double avgSpeedupVsBruteForce; // null if nothing verified
    }

    static List<WindowSummary> summarize(List<QueryRow> rows) {
        TreeMap<Double, List<QueryRow>> byWindow = new TreeMap<>();
        for (QueryRow row : rows) {
            byWindow.computeIfAbsent(row.windowM, k -> new ArrayList<>()).add(row);
        }

        List<WindowSummary> summaries = new ArrayList<>();
        for (Map.Entry<Double, List<QueryRow>> entry : byWindow.entrySet()) {
            List<QueryRow> group = entry.getValue();

            List<Double> totalTimes = group.stream().map(r -> r.totalTimeS).collect(Collectors.toList());
            List<Integer> candidateCounts = group.stream().map(r -> r.numCandidates).collect(Collectors.toList());
            List<Integer> exactCounts = group.stream().map(r -> r.numExact).collect(Collectors.toList());
            List<Double> precisionValues =
                    group.stream()
                            .map(r -> r.numCandidates > 0 ? (double) r.numExact / r.numCandidates : 1.0)
                            .collect(Collectors.toList());

            List<QueryRow> verifiedRows = group.stream().filter(r -> r.verified).collect(Collectors.toList());
            int numVerified = verifiedRows.size();
            int numCorrect = (int) verifiedRows.stream().filter(r -> Boolean.TRUE.equals(r.correct)).count();
            List<Double> bfTimes =
                    verifiedRows.stream().map(r -> r.bruteForceTimeS).filter(x -> x != null).collect(Collectors.toList());
            List<Double> verifiedTotalTimes = verifiedRows.stream().map(r -> r.totalTimeS).collect(Collectors.toList());

            WindowSummary s = new WindowSummary();
            s.windowM = entry.getKey();
            s.numQueries = group.size();
            s.p50TotalTimeS = percentile(totalTimes, 50);
            s.p90TotalTimeS = percentile(totalTimes, 90);
            s.p99TotalTimeS = percentile(totalTimes, 99);
            s.avgCandidates = meanInt(candidateCounts);
            s.avgExactMatches = meanInt(exactCounts);
            s.avgPrecision = meanDouble(precisionValues);
            s.numVerified = numVerified;
            s.numCorrect = numCorrect;

            if (!bfTimes.isEmpty()) {
                double avgBf = meanDouble(bfTimes);
                double avgVerifiedTotal = meanDouble(verifiedTotalTimes);
                s.avgBruteForceTimeS = avgBf;
                s.avgSpeedupVsBruteForce = avgVerifiedTotal > 0 ? avgBf / avgVerifiedTotal : null;
            } else {
                s.avgBruteForceTimeS = null;
                s.avgSpeedupVsBruteForce = null;
            }

            summaries.add(s);
        }
        return summaries;
    }

    private static double meanInt(List<Integer> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        long sum = 0;
        for (int v : values) {
            sum += v;
        }
        return (double) sum / values.size();
    }

    private static double meanDouble(List<Double> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.size();
    }

    static void printQueryReport(List<WindowSummary> summaries) {
        System.out.println("Spatial range query benchmark");
        System.out.println("------------------------------");
        String header = String.format(
                "%10s %5s %9s %9s %9s %9s %10s %10s %14s %10s",
                "window(m)", "#q", "p50(ms)", "p90(ms)", "p99(ms)", "avg_cand", "avg_exact", "precision", "speedup_vs_bf", "correct");
        System.out.println(header);
        // String.repeat() is Java 11+; build the separator manually so this
        // file keeps working on Java 8.
        StringBuilder separator = new StringBuilder(header.length());
        for (int i = 0; i < header.length(); i++) {
            separator.append('-');
        }
        System.out.println(separator);

        for (WindowSummary s : summaries) {
            String speedup = s.avgSpeedupVsBruteForce != null ? String.format("%.1fx", s.avgSpeedupVsBruteForce) : "n/a";
            String correct = s.numVerified > 0 ? s.numCorrect + "/" + s.numVerified : "n/a";
            System.out.printf(
                    "%10.0f %5d %9.3f %9.3f %9.3f %9.1f %10.1f %10.2f %14s %10s%n",
                    s.windowM,
                    s.numQueries,
                    s.p50TotalTimeS * 1000,
                    s.p90TotalTimeS * 1000,
                    s.p99TotalTimeS * 1000,
                    s.avgCandidates,
                    s.avgExactMatches,
                    s.avgPrecision,
                    speedup,
                    correct);
        }
        System.out.println();
        System.out.println("precision = avg_exact_matches / avg_candidates (1.0 = the TShape filter produced zero false positives)");
        System.out.println("speedup_vs_bf = brute-force (no index) query time / TShape query time, measured only on spot-checked queries");
    }

    static void saveCsv(List<QueryRow> rows, String path) throws IOException {
        File outFile = new File(path);
        if (outFile.getParentFile() != null) {
            outFile.getParentFile().mkdirs();
        }
        try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
            pw.println("window_m,query_id,num_candidates,num_exact,candidate_time_s,exact_time_s,total_time_s,verified,correct,brute_force_time_s");
            for (QueryRow r : rows) {
                pw.println(
                        r.windowM
                                + ","
                                + r.queryId
                                + ","
                                + r.numCandidates
                                + ","
                                + r.numExact
                                + ","
                                + r.candidateTimeS
                                + ","
                                + r.exactTimeS
                                + ","
                                + r.totalTimeS
                                + ","
                                + r.verified
                                + ","
                                + (r.correct == null ? "" : r.correct)
                                + ","
                                + (r.bruteForceTimeS == null ? "" : r.bruteForceTimeS));
            }
        }
        System.out.println("Saved raw per-query results to " + path);
    }

    static void saveJsonSummary(
            List<WindowSummary> summaries,  Map<String, Object> indexStats, String path)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");

        sb.append("  \"index_stats\": ");
        Json.write(indexStats, sb, 2);
        sb.append(",\n");

        sb.append("  \"query_summary_by_window\": [\n");
        for (int i = 0; i < summaries.size(); i++) {
            WindowSummary s = summaries.get(i);
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("window_m", s.windowM);
            m.put("num_queries", s.numQueries);
            m.put("p50_total_time_s", s.p50TotalTimeS);
            m.put("p90_total_time_s", s.p90TotalTimeS);
            m.put("p99_total_time_s", s.p99TotalTimeS);
            m.put("avg_candidates", s.avgCandidates);
            m.put("avg_exact_matches", s.avgExactMatches);
            m.put("avg_precision", s.avgPrecision);
            m.put("num_verified", s.numVerified);
            m.put("num_correct", s.numCorrect);
            m.put("avg_brute_force_time_s", s.avgBruteForceTimeS);
            m.put("avg_speedup_vs_brute_force", s.avgSpeedupVsBruteForce);
            sb.append("    ");
            Json.write(m, sb, 4);
            sb.append(i < summaries.size() - 1 ? ",\n" : "\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");

        File outFile = new File(path);
        if (outFile.getParentFile() != null) {
            outFile.getParentFile().mkdirs();
        }
        Files.write(outFile.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("Saved aggregated summary to " + path);
    }

    /**
     * Minimal, purpose-built JSON writer (handles exactly the value shapes
     * this file produces: Map, List, String, Boolean, and numbers). Avoids
     * pulling in a third-party JSON library, per the "plain .java files
     * only" packaging choice.
     */
    private static final class Json {
        @SuppressWarnings("unchecked")
        static void write(Object value, StringBuilder sb, int indent) {
            if (value == null) {
                sb.append("null");
            } else if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                if (map.isEmpty()) {
                    sb.append("{}");
                    return;
                }
                sb.append("{\n");
                int i = 0;
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    indent(sb, indent + 2);
                    sb.append('"').append(escape(String.valueOf(e.getKey()))).append("\": ");
                    write(e.getValue(), sb, indent + 2);
                    i++;
                    sb.append(i < map.size() ? ",\n" : "\n");
                }
                indent(sb, indent);
                sb.append("}");
            } else if (value instanceof List) {
                List<?> list = (List<?>) value;
                if (list.isEmpty()) {
                    sb.append("[]");
                    return;
                }
                sb.append("[\n");
                for (int i = 0; i < list.size(); i++) {
                    indent(sb, indent + 2);
                    write(list.get(i), sb, indent + 2);
                    sb.append(i < list.size() - 1 ? ",\n" : "\n");
                }
                indent(sb, indent);
                sb.append("]");
            } else if (value instanceof String) {
                sb.append('"').append(escape((String) value)).append('"');
            } else if (value instanceof Boolean || value instanceof Number) {
                sb.append(value.toString());
            } else {
                // Fallback: stringify anything unexpected rather than crash
                // the whole report.
                sb.append('"').append(escape(value.toString())).append('"');
            }
        }

        private static void indent(StringBuilder sb, int n) {
            for (int i = 0; i < n; i++) {
                sb.append(' ');
            }
        }

        private static String escape(String s) {
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    // =====================================================================
    // Reporting: charts (pure java.awt, no plotting library)
    // =====================================================================

    static void savePlots(List<WindowSummary> summaries, String path) throws IOException {
        if (summaries.isEmpty()) {
            return;
        }

        int width = 1100;
        int height = 450;
        int panelWidth = width / 2;
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = image.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setColor(Color.WHITE);
        g2.fillRect(0, 0, width, height);

        List<Double> windows = summaries.stream().map(s -> s.windowM).collect(Collectors.toList());
        List<Double> p50 = summaries.stream().map(s -> s.p50TotalTimeS * 1000).collect(Collectors.toList());
        List<Double> p90 = summaries.stream().map(s -> s.p90TotalTimeS * 1000).collect(Collectors.toList());
        List<Double> candidates = summaries.stream().map(s -> s.avgCandidates).collect(Collectors.toList());
        List<Double> exact = summaries.stream().map(s -> s.avgExactMatches).collect(Collectors.toList());

        drawLineChart(
                g2,
                0,
                0,
                panelWidth,
                height,
                "Query latency vs. window size",
                "window size (m)",
                "query time (ms)",
                windows,
                Arrays.asList(new SeriesData("p50", p50, new Color(31, 119, 180)), new SeriesData("p90", p90, new Color(255, 127, 14))));

        drawLineChart(
                g2,
                panelWidth,
                0,
                panelWidth,
                height,
                "Candidates vs. exact matches",
                "window size (m)",
                "count",
                windows,
                Arrays.asList(
                        new SeriesData("avg candidates (TShape filter)", candidates, new Color(31, 119, 180)),
                        new SeriesData("avg exact matches", exact, new Color(255, 127, 14))));

        g2.dispose();

        File outFile = new File(path);
        if (outFile.getParentFile() != null) {
            outFile.getParentFile().mkdirs();
        }
        ImageIO.write(image, "png", outFile);
        System.out.println("Saved charts to " + path);
    }

    private static final class SeriesData {
        final String label;
        final List<Double> values;
        final Color color;

        SeriesData(String label, List<Double> values, Color color) {
            this.label = label;
            this.values = values;
            this.color = color;
        }
    }

    /** Draw one simple multi-series line chart with axis labels, a title,
     *  and a legend, inside the given panel rectangle. Hand-rolled with
     *  java.awt.Graphics2D -- no charting library. */
    private static void drawLineChart(
            Graphics2D g2,
            int panelX,
            int panelY,
            int panelW,
            int panelH,
            String title,
            String xLabel,
            String yLabel,
            List<Double> xValues,
            List<SeriesData> series) {
        int marginLeft = 70;
        int marginRight = 30;
        int marginTop = 50;
        int marginBottom = 60;

        int plotX = panelX + marginLeft;
        int plotY = panelY + marginTop;
        int plotW = panelW - marginLeft - marginRight;
        int plotH = panelH - marginTop - marginBottom;

        double xMin = xValues.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        double xMax = xValues.stream().mapToDouble(Double::doubleValue).max().orElse(1);
        double yMax = 0;
        for (SeriesData s : series) {
            for (double v : s.values) {
                yMax = Math.max(yMax, v);
            }
        }
        if (xMax <= xMin) {
            xMax = xMin + 1;
        }
        if (yMax <= 0) {
            yMax = 1;
        }
        // A little headroom above the tallest point.
        yMax *= 1.15;

        g2.setColor(Color.BLACK);
        g2.setFont(new Font("SansSerif", Font.BOLD, 14));
        g2.drawString(title, plotX, panelY + 24);

        // Axes.
        g2.setColor(new Color(60, 60, 60));
        g2.setStroke(new BasicStroke(1.5f));
        g2.drawLine(plotX, plotY, plotX, plotY + plotH);
        g2.drawLine(plotX, plotY + plotH, plotX + plotW, plotY + plotH);

        g2.setFont(new Font("SansSerif", Font.PLAIN, 11));
        g2.drawString(xLabel, plotX + plotW / 2 - 30, plotY + plotH + 36);
        Graphics2D g2r = (Graphics2D) g2.create();
        g2r.rotate(-Math.PI / 2);
        g2r.drawString(yLabel, -(plotY + plotH / 2 + 20), plotX - 45);
        g2r.dispose();

        // Y axis ticks (5 gridlines).
        for (int i = 0; i <= 4; i++) {
            double frac = i / 4.0;
            int y = plotY + plotH - (int) Math.round(frac * plotH);
            double value = frac * yMax;
            g2.setColor(new Color(225, 225, 225));
            g2.drawLine(plotX, y, plotX + plotW, y);
            g2.setColor(Color.BLACK);
            g2.drawString(String.format("%.0f", value), plotX - 42, y + 4);
        }

        // Series.
        int legendY = panelY + 40;
        for (SeriesData s : series) {
            g2.setColor(s.color);
            g2.setStroke(new BasicStroke(2.2f));
            Integer prevPx = null;
            Integer prevPy = null;
            for (int i = 0; i < xValues.size(); i++) {
                double xFrac = (xValues.get(i) - xMin) / (xMax - xMin);
                double yFrac = s.values.get(i) / yMax;
                int px = plotX + (int) Math.round(xFrac * plotW);
                int py = plotY + plotH - (int) Math.round(yFrac * plotH);
                if (prevPx != null) {
                    g2.drawLine(prevPx, prevPy, px, py);
                }
                g2.fillOval(px - 3, py - 3, 6, 6);
                prevPx = px;
                prevPy = py;
            }

            // Legend entry.
            g2.fillRect(plotX + plotW - 180, legendY - 8, 12, 12);
            g2.setColor(Color.BLACK);
            g2.drawString(s.label, plotX + plotW - 162, legendY + 2);
            legendY += 18;
        }
    }

    // =====================================================================
    // CLI / main
    // =====================================================================

    /** Parsed command-line configuration. Defaults mirror the Python
     *  script's tightened defaults (see the module comment above) so a bare
     *  `java GeolifeExperiment` finishes quickly. */
    static final class Args {
        String geolifePath = null; // required (no cross-platform sensible default in Java)
        Integer maxTrajectories = 1000;
        int minPoints = 10;
        Integer maxPointsPerTrajectory = 500; // 0 or negative means "no limit"
        int alpha = 3;
        int beta = 3;
        int maxResolution = 16;
        int greedyStarts = 16;
        long greedyWorkBudget = 2_000_000L;
        List<Double> windowSizesM = new ArrayList<>(Arrays.asList(100.0, 500.0, 1000.0));
        int numQueries = 20;
        int verifyEvery = 8;
        boolean noVerify = false;
        long seed = 42L;
        String outputDir = "reports";
        boolean noSave = false;
        boolean noPlot = false;
        String plotTrajectory = null;
    }

    private static void printHelp() {
        System.out.println("Usage: java GeolifeExperiment --geolife-path <dir> [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --geolife-path <dir>            Path to the GeoLife 'Data' directory (required)");
        System.out.println("  --max-trajectories <n>          Cap on how many .plt files to load (default 1000)");
        System.out.println("  --min-points <n>                Skip trajectories shorter than this (default 10)");
        System.out.println("  --max-points-per-trajectory <n> Truncate trajectories to their first N points, 0 = no limit (default 500)");
        System.out.println("  --alpha <n>                     TShape enlarged-element width, in cells (default 3)");
        System.out.println("  --beta <n>                      TShape enlarged-element height, in cells (default 3)");
        System.out.println("  --max-resolution <n>            Max quadtree depth (default 16)");
        System.out.println("  --greedy-starts <n>             Multi-start greedy shape-order attempts (default 16)");
        System.out.println("  --greedy-work-budget <n>        Cap on starts*shapes^2 per element (default 2000000)");
        System.out.println("  --window-sizes-m <n...>         Query window sizes in meters (default 100 500 1000)");
        System.out.println("  --num-queries <n>                Random queries per window size (default 20)");
        System.out.println("  --verify-every <n>               Spot-check every Nth query against brute force (default 8)");
        System.out.println("  --no-verify                      Skip brute-force ground-truth checking entirely");
        System.out.println("  --seed <n>                       Random seed (default 42)");
        System.out.println("  --output-dir <dir>                Where to write CSV/JSON/PNG reports (default ./reports)");
        System.out.println("  --no-save                        Only print the report; don't write any files");
        System.out.println("  --no-plot                        Skip chart generation");
        System.out.println("  --plot-trajectory <id>            Also save a TShape plot for this trajectory id");
        System.out.println("  --help                            Show this message");
    }

    private static Args parseArgs(String[] argv) {
        Args args = new Args();
        int i = 0;
        while (i < argv.length) {
            String flag = argv[i];
            switch (flag) {
                case "--help":
                    printHelp();
                    System.exit(0);
                    break;
                case "--geolife-path":
                    args.geolifePath = argv[++i];
                    break;
                case "--max-trajectories":
                    args.maxTrajectories = Integer.parseInt(argv[++i]);
                    break;
                case "--min-points":
                    args.minPoints = Integer.parseInt(argv[++i]);
                    break;
                case "--max-points-per-trajectory":
                    args.maxPointsPerTrajectory = Integer.parseInt(argv[++i]);
                    break;
                case "--alpha":
                    args.alpha = Integer.parseInt(argv[++i]);
                    break;
                case "--beta":
                    args.beta = Integer.parseInt(argv[++i]);
                    break;
                case "--max-resolution":
                    args.maxResolution = Integer.parseInt(argv[++i]);
                    break;
                case "--greedy-starts":
                    args.greedyStarts = Integer.parseInt(argv[++i]);
                    break;
                case "--greedy-work-budget":
                    args.greedyWorkBudget = Long.parseLong(argv[++i]);
                    break;
                case "--window-sizes-m": {
                    List<Double> sizes = new ArrayList<>();
                    while (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                        sizes.add(Double.parseDouble(argv[++i]));
                    }
                    if (!sizes.isEmpty()) {
                        args.windowSizesM = sizes;
                    }
                    break;
                }
                case "--num-queries":
                    args.numQueries = Integer.parseInt(argv[++i]);
                    break;
                case "--verify-every":
                    args.verifyEvery = Integer.parseInt(argv[++i]);
                    break;
                case "--no-verify":
                    args.noVerify = true;
                    break;
                case "--seed":
                    args.seed = Long.parseLong(argv[++i]);
                    break;
                case "--output-dir":
                    args.outputDir = argv[++i];
                    break;
                case "--no-save":
                    args.noSave = true;
                    break;
                case "--no-plot":
                    args.noPlot = true;
                    break;
                case "--plot-trajectory":
                    args.plotTrajectory = argv[++i];
                    break;
                default:
                    System.err.println("Unknown argument: " + flag + " (use --help for usage)");
                    System.exit(1);
            }
            i++;
        }

        if (args.geolifePath == null) {
            System.err.println("--geolife-path is required (use --help for usage)");
            System.exit(1);
        }
        return args;
    }

    public static void main(String[] argv) throws IOException {
        Args args = parseArgs(argv);
        Random rng = new Random(args.seed);

        Integer maxPointsPerTrajectory =
                (args.maxPointsPerTrajectory != null && args.maxPointsPerTrajectory > 0) ? args.maxPointsPerTrajectory : null;

        // --- Load ---------------------------------------------------------
        System.out.println("Loading up to " + args.maxTrajectories + " trajectories from " + args.geolifePath + " ...");
        long t0 = System.nanoTime();
        LinkedHashMap<String, List<Point>> trajectories =
                loadGeolife(args.geolifePath, args.maxTrajectories, args.minPoints, maxPointsPerTrajectory);
        double loadTime = (System.nanoTime() - t0) / 1_000_000_000.0;
        System.out.printf("Loaded %d trajectories in %.2fs%n", trajectories.size(), loadTime);
        System.out.println();
        printDatasetStats(trajectories);
        System.out.println();

        if (trajectories.isEmpty()) {
            System.out.println("No trajectories loaded -- check --geolife-path. Aborting.");
            return;
        }

        // --- Build ----------------------------------------------------------
        TShapeIndex index =
                new TShapeIndex(
                        args.alpha, args.beta, args.maxResolution, null, 16, args.greedyStarts, args.greedyWorkBudget, args.seed);
        index.build(trajectories);
        System.out.println();
        index.printStats();
        System.out.println();

        // --- Query benchmark --------------------------------------------
        boolean verify = !args.noVerify;
        String verifyDesc =
                verify ? "spot-checking every " + args.verifyEvery + "th query against brute force" : "brute-force verification disabled (--no-verify)";
        System.out.println(
                "Running query benchmark: "
                        + args.windowSizesM.size()
                        + " window sizes x "
                        + args.numQueries
                        + " queries each ("
                        + verifyDesc
                        + ") ...");
        t0 = System.nanoTime();
        List<QueryRow> rows =
                runQueryBenchmark(index, trajectories, args.windowSizesM, args.numQueries, rng, Math.max(1, args.verifyEvery), verify);
        double benchmarkTime = (System.nanoTime() - t0) / 1_000_000_000.0;
        System.out.printf("Benchmark finished in %.2fs (%d queries total)%n", benchmarkTime, rows.size());
        System.out.println();

        List<WindowSummary> summaries = summarize(rows);
        printQueryReport(summaries);

        boolean anyIncorrect = summaries.stream().anyMatch(s -> s.numCorrect != s.numVerified);
        if (anyIncorrect) {
            System.out.println();
            System.out.println(
                    "WARNING: at least one spot-checked query did not match the brute-force ground truth. "
                            + "See the CSV export for details.");
        }

        // --- Save artifacts -----------------------------------------------
        if (!args.noSave) {
            System.out.println();
            saveCsv(rows, args.outputDir + File.separator + "geolife_benchmark.csv");
            saveJsonSummary(
                    summaries,
                    index.stats(),
                    args.outputDir + File.separator + "geolife_benchmark_summary.json");
            if (!args.noPlot) {
                savePlots(summaries, args.outputDir + File.separator + "geolife_benchmark.png");
            }
        }

        // --- Optional single-trajectory plot ------------------------------
        if (args.plotTrajectory != null) {
            if (index.records.containsKey(args.plotTrajectory)) {
                String outPath = args.outputDir + File.separator + args.plotTrajectory + "_tshape.png";
                index.plotTrajectoryTShape(args.plotTrajectory, outPath);
            } else {
                System.out.println("Trajectory id '" + args.plotTrajectory + "' not found in the index; skipping plot.");
            }
        }
    }
}
