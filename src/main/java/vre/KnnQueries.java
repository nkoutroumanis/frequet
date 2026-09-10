package vre;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.vre.VRERecord;
import gr.ds.unipi.spatialnodb.messages.common.vre.VRERecordMetadataReadSupport;
import gr.ds.unipi.spatialnodb.messages.common.vre.VRERecordSegmentPointsReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import vre.impl.SignatureCoder;
import vre.impl.XZ2Coder;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static vre.DataLoading.pivots;

public class KnnQueries {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries-vre.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final int k = dataLoading.getInt("k");

        Config metadata;
        if (parquetPath.startsWith("hdfs://")) {
            Path filePath = new Path(parquetPath, "space.metadata");
            FileSystem fs = filePath.getFileSystem(new Configuration());
            try (InputStream in = fs.open(filePath)) {
                metadata = ConfigFactory.parseReader(
                        new InputStreamReader(in)
                ).resolve().getConfig("vre");
            }
        } else {
            metadata = ConfigFactory.parseFile(new File(parquetPath + File.separator + "space.metadata")).resolve().getConfig("vre");
        }

        Config boundaries = metadata.getConfig("boundaries");
        final double minLon = boundaries.getDouble("minLon");
        final double minLat = boundaries.getDouble("minLat");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");

        final int maxResolution = metadata.getInt("maxResolution");
        final int m = metadata.getInt("m");
        final int n = metadata.getInt("n");

        Config stats = metadata.getConfig("stats");
        final long totalNumberOfTracklets = stats.getLong("totalNumberOfTracklets");
        final long numOfTrajectories = stats.getLong("numOfTrajectories");
        final int segNum = (int) (Math.ceil((double) totalNumberOfTracklets /numOfTrajectories)/2);

        Job jobMeta = Job.getInstance();
        Job jobSegmentPoints = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(jobMeta, VRERecordMetadataReadSupport.class);
        ParquetInputFormat.setReadSupportClass(jobSegmentPoints, VRERecordSegmentPointsReadSupport.class);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Similarity Querying in TMan");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory", "4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        XZ2Coder index = new XZ2Coder(maxResolution, minLon, maxLon, minLat, maxLat);
        SignatureCoder signatureCoder = new SignatureCoder(m, n);

        HashSet<Long> existingCodes;
        if (parquetPath.startsWith("hdfs://")) {
            FileSystem fs = new Path(parquetPath).getFileSystem(jobMeta.getConfiguration());
            try (FSDataInputStream hdfsIn = fs.open(new Path(parquetPath+File.separator+"cache.ser"));
                 ObjectInputStream in = new ObjectInputStream(hdfsIn)) {
                existingCodes = (HashSet<Long>) in.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }else{
            try (ObjectInputStream in = new ObjectInputStream(
                    new FileInputStream(parquetPath+File.separator+"cache.ser"))) {
                existingCodes = (HashSet<Long>) in.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        String fullPathExportedFile = metricsPath+ File.separator+"knn-"+k+"-queries-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tQuery Points\tNum of Trajectories\tk-th Distance\tNum of Points\tData Pages\tParse\tMetadata Rounds\tTrajectory Query Rounds\n");
        String query;
        while ((query = br.readLine()) != null) {
            long startTime = System.currentTimeMillis();

            int pointsCount = countPoints(query);
            SpatialPoint[] trajectoryQuery = new SpatialPoint[pointsCount];
            char[] chars = query.toCharArray();
            int ichar = 0;
            int idx = 0;
            int len = chars.length;

            double mbrMinLongitude = Double.MAX_VALUE;
            double mbrMinLatitude= Double.MAX_VALUE;
            long mbrMinTimestamp= Long.MAX_VALUE;

            double mbrMaxLongitude= -Double.MAX_VALUE;
            double mbrMaxLatitude= -Double.MAX_VALUE;
            long mbrMaxTimestamp= Long.MIN_VALUE;

            while (ichar < len) {
                // Parse longitude
                int start = ichar;
                while (chars[ichar] != ',') ichar++;
                double lon = parseDouble(chars, start, ichar);
                ichar++; // skip ','

                // Parse latitude
                start = ichar;
                while (chars[ichar] != ',') ichar++;
                double lat = parseDouble(chars, start, ichar);
                ichar++; // skip ','

                // Parse timestamp
                start = ichar;
                while (chars[ichar] != ';') ichar++;
                long tstp = parseLong(chars, start, ichar);
                ichar++; // skip ';'

                trajectoryQuery[idx++] = new SpatialPoint(lon,lat);

                if(Double.compare(lon,mbrMaxLongitude)==1){
                    mbrMaxLongitude = lon;
                }
                if(Double.compare(lat,mbrMaxLatitude)==1){
                    mbrMaxLatitude = lat;
                }
                if(Double.compare(tstp,mbrMaxTimestamp)==1){
                    mbrMaxTimestamp = tstp;
                }

                if(Double.compare(lon,mbrMinLongitude)==-1){
                    mbrMinLongitude = lon;
                }
                if(Double.compare(lat,mbrMinLatitude)==-1){
                    mbrMinLatitude = lat;
                }
                if(Double.compare(tstp,mbrMinTimestamp)==-1){
                    mbrMinTimestamp = tstp;
                }
            }

//            List<Double> epsilons = new ArrayList<>();
//            List<Integer> res = new ArrayList<>();
            int metadataRounds=0;

            double qXmin = mbrMinLongitude, qYmin = mbrMinLatitude;
            double qXmax = mbrMaxLongitude, qYmax = mbrMaxLatitude;

            double[] qMbr = new double[]{qXmin, qYmin, qXmax, qYmax};
            SpatialPoint[] qPivots = pivots(trajectoryQuery, qXmin, qYmin, qXmax, qYmax);
            byte[] querySignature = signatureCoder.signature(trajectoryQuery, qXmin, qYmin, qXmax, qYmax);

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            PriorityQueue<CellEntry> mbrq = new PriorityQueue<CellEntry>(
                    new Comparator<CellEntry>() {
                        @Override public int compare(CellEntry a, CellEntry b) {
                            return Double.compare(a.minDist, b.minDist);
                        }
                    });
            PriorityQueue<Result> result = new PriorityQueue<Result>(
                    new Comparator<Result>() {
                        @Override public int compare(Result a, Result b) {
                            return Double.compare(b.distance, a.distance); // max-heap
                        }
                    });

            double epsilon = Double.POSITIVE_INFINITY;
            Set<String> verified = new HashSet<String>();
            Set<String> pruned = new HashSet<String>();

            Map<String, List<VRERecord>> ct = new HashMap<String, List<VRERecord>>();
            Set<String> seen = new HashSet<String>();
            seedAllLevelsDistanceZero(mbrq, seen, index, minLon, minLat, maxLon, maxLat, qXmin, qYmin, qXmax, qYmax, trajectoryQuery);
//            mbrq.add(new CellEntry(new int[0], minLon, minLat, maxLon, maxLat, minDistToRect(minLon, minLat, maxLon, maxLat, trajectoryQuery)));
//            int[] rootSeq = new int[0];
//            seen.add(Arrays.toString(rootSeq));

            //            seedAllLevelsDistanceZero(mbrq, index, minLon, minLat, maxLon, maxLat,
//                    qXmin, qYmin, qXmax, qYmax, trajectoryQuery);
            while (!mbrq.isEmpty()) {
                if (result.size() >= k && mbrq.peek().minDist >= epsilon) {
                    break;
                }

                double d0 = mbrq.peek().minDist;
                List<CellEntry> batch = new ArrayList<CellEntry>();
                while (!mbrq.isEmpty() && Double.compare(mbrq.peek().minDist, d0) == 0) {
                    CellEntry e = mbrq.poll();
                    batch.add(e);
                }
//                while (!mbrq.isEmpty() && Double.compare(mbrq.peek().minDist, d0) == 0) {
//                    CellEntry e = mbrq.poll();
//                    if (result.size() >= k && epsilon != Double.POSITIVE_INFINITY) {
//                        double xLen = e.x1 - e.x0, yLen = e.y1 - e.y0;
//                        double ex0 = e.x0 - epsilon, ey0 = e.y0 - epsilon;
//                        double ex1 = (e.x1 + xLen) + epsilon, ey1 = (e.y1 + yLen) + epsilon;
//                        boolean keep = ex0 <= qXmin && ey0 <= qYmin
//                                && ex1 >= qXmax && ey1 >= qYmax;
//                        if (!keep) {
//                            continue;
//                        }
//                    }
//                    batch.add(e);
//                }
                if (batch.isEmpty()) {
                    continue;
                }

                //one batched store query for the whole equal-distance group
                Set<Long> codes = new HashSet<>(batch.size());
                for (int i = 0; i < batch.size(); i++) {
                    CellEntry e = batch.get(i);
                    long code = index.quadrantCode(e.seq);
                    if (existingCodes.contains(code)) {
                        codes.add(code);
                    }
                }
                if (!codes.isEmpty()) {
                    ParquetInputFormat.setFilterPredicate(jobMeta.getConfiguration(), in(longColumn("key"), codes));
                    JavaPairRDD<Void, VRERecord> candidates = (JavaPairRDD<Void, VRERecord>) jsc.newAPIHadoopFile(parquetPath + File.separator + "sIndex", ParquetInputFormat.class, Void.class, VRERecord.class, jobMeta.getConfiguration());
                    List<VRERecord> cands = candidates.map(f -> f._2).collect();
                    metadataRounds++;

                    for (VRERecord rec : cands) {
                        String oid = rec.getObjectId();
                        if (verified.contains(oid) || pruned.contains(oid)) {
                            continue;
                        }

                        // Accumulate into C_t FIRST so the group holds all segments seen so far.
                        List<VRERecord> group = ct.get(oid);
                        if (group == null) {
                            group = new ArrayList<VRERecord>();
                            ct.put(oid, group);
                        }
                        group.add(rec);

                        boolean isFull = isFull(group);
                        if (result.size() >= k) {
                            boolean prune = false;
                            if (pruneByLowerBound(trajectoryQuery, qPivots, qMbr, querySignature, group, m, n, epsilon, isFull)) {
                                prune = true;
                            }

                            if (prune) {
                                pruned.add(oid);
                                ct.remove(oid);
                                continue;
                            }
                        }

//                        if(group.get(0).getObjectId().equals("197387") && (group.size()==2)){
//                            System.out.println(group.size());
//                            System.out.println(group.size() >= segNum);
//                            System.out.println(isFull);
//                            group.forEach(s-> System.out.println(s));
//                            System.exit(12);
//                        }

                        if (/*group.size() >= segNum || */isFull) {
                            ParquetInputFormat.setFilterPredicate(jobSegmentPoints.getConfiguration(), eq(binaryColumn("objectId"), Binary.fromString(oid)));
                            JavaPairRDD<Void, VRERecord> wholeSegments = (JavaPairRDD<Void, VRERecord>) jsc.newAPIHadoopFile(parquetPath + File.separator + "sIndex", ParquetInputFormat.class, Void.class, VRERecord.class, jobSegmentPoints.getConfiguration());
                            JavaPairRDD<Double, VRERecord> results = (JavaPairRDD<Double, VRERecord>) wholeSegments.mapToPair(f -> Tuple2.apply(f._2.getObjectId(), f._2)).groupByKey(Integer.parseInt(args[0])).map(f -> {
                                int count = 0;
                                for (VRERecord vreRecord : f._2) {
                                    count++;
                                }
                                List<VRERecord> records = new ArrayList<>(count);
                                f._2.forEach(records::add);
                                records.sort(Comparator.comparingLong(VRERecord::getSerialNumber));

                                return new VRERecord(f._1, records);
                            }).mapToPair(f -> Tuple2.apply(HilbertUtil.frechetDistance(trajectoryQuery, f.getSpatialPoints()), f));

                            for (Tuple2<Double, VRERecord> tuple2 : results.collect()) {
                                verified.add(oid);
                                ct.remove(oid);
                                if (result.size() < k) {
                                    result.add(new Result(oid, tuple2._2.getSpatialPoints(), tuple2._1));
                                } else if (tuple2._1 < result.peek().distance) {
                                    result.poll();
                                    result.add(new Result(oid, tuple2._2.getSpatialPoints(), tuple2._1));
                                }
                            }
                            if (result.size() >= k) {
                                epsilon = result.peek().distance;
                            }
                        }
                    }
                }
//                if (result.size() >= k) {
//                    epsilon = result.peek().distance;
//                }

//                epsilons.add(epsilon);
//                res.add(result.size());
                // ---- expand every batched cell's children -----------------------
                for (CellEntry e : batch) {
                    if (e.seq.length < index.g) {
                        for (int q = 0; q < 4; q++) {
                            double[] cb = childBounds(e.x0, e.y0, e.x1, e.y1, q);
                            int[] cs = new int[e.seq.length + 1];
                            System.arraycopy(e.seq, 0, cs, 0, e.seq.length);
                            cs[e.seq.length] = q;
                            String key = Arrays.toString(cs);
                            if (!seen.add(key)) {        // already seeded/added -> skip
                                continue;
                            }
                            double mdee = minDistToRect(cb[0], cb[1], cb[2], cb[3], trajectoryQuery);
                            mbrq.add(new CellEntry(cs, cb[0], cb[1], cb[2], cb[3], mdee));
                        }
                    }
                }
            }

//            for (Result result1 : result) {
//                System.out.println(result1.distance);
//                System.out.println(result1.objectId+" "+Arrays.toString(result1.trajectory));
//
//            }
            //            System.out.println(verified.size());
//            System.out.println(epsilons);
//            System.out.println(res);
//            System.out.println(metadataRounds);

//            List<Tuple2<Void,TManRecord>> trajs = pairRDDRangeQuery.collect();
            long endTime = System.currentTimeMillis();
//            long num = trajs.size();
//
            long numOfPoints = 0;
            for (Result voidTrajectoryTuple2 : result) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2.trajectory.length;
            }

            bw.write((endTime - startTime)+"\t"+trajectoryQuery.length+"\t"+result.size()+"\t"+result.peek().distance+"\t"+numOfPoints+"\t"+DataPage.counter+"\t"+parseAndCubeIndex+"\t"+metadataRounds+"\t"+ verified.size());
            DataPage.counter = 0;
            bw.newLine();
        }
        bw.close();
        br.close();

        String applicationId = sparkSession.sparkContext().applicationId();

        sparkSession.close();

        br = new BufferedReader(new FileReader(fullPathExportedFile));
        List<Integer> queryEndJobs = new ArrayList<>();
        br.readLine();
        String line;
        while ((line = br.readLine()) != null) {
            String[] values = line.split("\t");
            int valuesNum = values.length;
            int last = Integer.parseInt(values[valuesNum - 1]);
            int previous = Integer.parseInt(values[valuesNum - 2]);
            queryEndJobs.add(last+previous);
        }

        if(sparkConf.getBoolean("spark.eventLog.enabled",false)){
            String eventLogDir = sparkConf.get("spark.eventLog.dir");
            File dir = new File(eventLogDir.replace("file:", ""));
            File eventLogFile =
                    Arrays.stream(dir.listFiles())
                            .filter(File::isFile)
                            .filter(f -> f.getName().contains(applicationId))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException(
                                    "No event log file found for application " + applicationId));

            List<Long>[] lists = SparkLogParser.getMetricsPerNJobs(eventLogFile.getAbsolutePath(), queryEndJobs);
            try {
                SparkLogParser.enrichQueryAdHocFileWithMetrics(fullPathExportedFile, lists);
            }catch (Exception e) {
                e.printStackTrace();
            }
            eventLogFile.delete();
        }
    }

    private static int countPoints(String line) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == ';') count++;
        }
        return count;
    }

    private static double parseDouble(char[] chars, int start, int end) {
        return Double.parseDouble(new String(chars, start, end - start));
    }

    // Parse a long directly from char[] between start (inclusive) and end (exclusive)
    private static long parseLong(char[] chars, int start, int end) {
        long result = 0;
        boolean neg = false;
        int i = start;
        if (chars[i] == '-') { neg = true; i++; }

        while (i < end && chars[i] >= '0' && chars[i] <= '9') {
            result = result * 10 + (chars[i] - '0');
            i++;
        }

        return neg ? -result : result;
    }
    private static final class CellEntry {
        final int[] seq;
        final double x0, y0, x1, y1;
        final double minDist;

        CellEntry(int[] seq, double x0, double y0, double x1, double y1, double d) {
            this.seq = seq;
            this.x0 = x0; this.y0 = y0; this.x1 = x1; this.y1 = y1;
            this.minDist = d;
        }
    }

    private static double minDistToRect(double x0, double y0, double x1, double y1,
                                        SpatialPoint[] q) {
        double best = Double.POSITIVE_INFINITY;
        for (SpatialPoint p : q) {
            double dx = Math.max(Math.max(x0 - p.getLongitude(), 0.0), p.getLongitude() - x1);
            double dy = Math.max(Math.max(y0 - p.getLatitude(), 0.0), p.getLatitude() - y1);
            best = Math.min(best, Math.sqrt(dx * dx + dy * dy));
        }
        return best;
    }

    private static double[] childBounds(double x0, double y0, double x1, double y1, int q) {
        double mx = (x0 + x1) / 2.0, my = (y0 + y1) / 2.0;
        switch (q) {
            case 0: return new double[]{x0, y0, mx, my};
            case 1: return new double[]{mx, y0, x1, my};
            case 2: return new double[]{x0, my, mx, y1};
            default: return new double[]{mx, my, x1, y1};
        }
    }

    private static boolean containsSerial(List<VRERecord> group, int serial) {
        for (VRERecord r : group) {
            if (r.getSerialNumber() == serial) {
                return true;
            }
        }
        return false;
    }

    private static boolean pruneByLowerBound(SpatialPoint[] q, SpatialPoint[] qPivots, double[] qMbr, byte[] querySignature, List<VRERecord> group, int m, int n, double epsilon, boolean isFull) {
        // LB_SES
        VRERecord start = null, end = null, full = null;

        for (VRERecord s : group) {
            if(s.getSegmentType()!=1){
                int t = s.getSegmentType();
                if (t == 0) start = s;
                else if (t == 2) end = s;
                else if (t == 3) full = s;

                if (full != null) {
                    start = full;
                    end = full;
                }
                if (start != null) {
                    double d = pointDistance(q[0], start.getFirstPoint());
                    if (d > epsilon) {
                        return true;
                    }
                }
                if (end != null) {
                    SpatialPoint[] ep = end.getSpatialPoints();
                    double d = pointDistance(q[q.length - 1], end.getLastPoint());
                    if (d > epsilon) {
                        return true;
                    }
                }
            }
        }

        // LB_Pivots
        if(isFull){
            for (SpatialPoint qj : qPivots) {
                double minOverSegs = Double.POSITIVE_INFINITY;
                for (VRERecord s : group) {
                    minOverSegs = Math.min(minOverSegs, pointRectDist(qj.getLongitude(), qj.getLatitude(), s.getMinLongitude(), s.getMinLatitude(), s.getMaxLongitude(), s.getMaxLatitude()));
                }
                if (minOverSegs > epsilon) {
                    return true;
                }
            }
        }

        //LB_PartialSim
        for (VRERecord s : group) {
            SpatialPoint segStart = s.getFirstPoint();//pts[0];
            SpatialPoint segEnd = s.getLastPoint();//pts[pts.length - 1];
            double dStart = Double.POSITIVE_INFINITY;
            double dEnd = Double.POSITIVE_INFINITY;
            for (SpatialPoint qj : q) {
                dStart = Math.min(dStart, pointDistance(qj, segStart));
                dEnd = Math.min(dEnd, pointDistance(qj, segEnd));
            }
            double segVal = Math.max(dStart, dEnd);
            if (segVal > epsilon) {
                return true;
            }
        }
        //LB_SIG
        for (VRERecord s : group) {
            for (int segIdx = 0; segIdx < m * n; segIdx++) {
                boolean segSet = (s.getSignature()[segIdx >> 3] & (1 << (segIdx & 7))) != 0;
                if (!segSet) {
                    continue;
                }

                int segCol = segIdx % n;
                int segRow = segIdx / n;

                double segCellW = (s.getMaxLongitude() - s.getMinLongitude()) / n;   // width  of one region
                double segCellH = (s.getMaxLatitude() - s.getMinLatitude()) / m;   // height of one region

                double segXmin = s.getMinLongitude() + segCol * segCellW;
                double segYmin = s.getMinLatitude() + segRow * segCellH;
                double segXmax = segXmin + segCellW;
                double segYmax = segYmin + segCellH;

                double distance = Double.POSITIVE_INFINITY;


                for (int idx = 0; idx < m * n; idx++) {
                    boolean set = (querySignature[idx >> 3] & (1 << (idx & 7))) != 0;
                    if (!set) {
                        continue;
                    }

                    int col = idx % n;
                    int row = idx / n;

                    double cellW = (qMbr[2] - qMbr[0]) / n;
                    double cellH = (qMbr[3] - qMbr[1]) / m;

                    double rXmin = qMbr[0] + col * cellW;
                    double rYmin = qMbr[1] + row * cellH;
                    double rXmax = rXmin + cellW;
                    double rYmax = rYmin + cellH;

                    distance = Math.min(distance, rectDist(rXmin, rYmin, rXmax, rYmax, segXmin, segYmin, segXmax, segYmax));
                }

                if (distance > epsilon) {
                    return true;
                }
            }
        }

        return false;
    }

    public static double pointDistance(SpatialPoint a, SpatialPoint b) {
        double dx = a.getLongitude() - b.getLongitude();
        double dy = a.getLatitude() - b.getLatitude();
        return Math.sqrt(dx * dx + dy * dy);
    }

    private static double pointRectDist(double px,double py, double x0,double y0,double x1,double y1){
        double dx = px<x0?x0-px:(px>x1?px-x1:0.0);
        double dy = py<y0?y0-py:(py>y1?py-y1:0.0);
        return Math.sqrt(dx*dx+dy*dy);
    }

    private static double rectDist(double xmin1, double ymin1, double xmax1, double ymax1, double xmin2, double ymin2, double xmax2, double ymax2) {
        double dx = Math.max(Math.max(xmin1 - xmax2, xmin2 - xmax1), 0.0);
        double dy = Math.max(Math.max(ymin1 - ymax2, ymin2 - ymax1), 0.0);
        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * isFull(C_t(tid)): whether the buffered segments form a complete trajectory.
     * A full (type 3) segment is complete on its own; otherwise the group must
     * contain the end segment (type 2) and a contiguous run of serial numbers
     * 0..max with no gaps.
     */
    private static boolean isFull(List<VRERecord> group) {
        boolean hasEnd = false;
        int maxSerial = -1;
        Set<Integer> serials = new HashSet<Integer>();
        for (VRERecord s : group) {
            if (s.getSegmentType() == 3) {   // full, unsplit trajectory
                return true;
            }
            if (s.getSegmentType() == 2) {   // end segment present
                hasEnd = true;
            }
            serials.add(s.getSerialNumber());
            if (s.getSerialNumber() > maxSerial) {
                maxSerial = s.getSerialNumber();
            }
        }
        if (!hasEnd) {
            return false;
        }
        for (int i = 1; i <= maxSerial; i++) {   // <-- start at 1, not 0
            if (!serials.contains(i)) {
                return false;   // a gap => not complete
            }
        }
        return true;
    }

    private static void seedAllLevelsDistanceZero(
            PriorityQueue<CellEntry> mbrq, XZ2Coder index,
            double minLon, double minLat, double maxLon, double maxLat,
            double qXmin, double qYmin, double qXmax, double qYmax,
            SpatialPoint[] trajectoryQuery) {
        Deque<CellEntry> stack = new ArrayDeque<>();
        stack.push(new CellEntry(new int[0], minLon, minLat, maxLon, maxLat,
                minDistToRect(minLon, minLat, maxLon, maxLat, trajectoryQuery)));
        while (!stack.isEmpty()) {
            CellEntry e = stack.pop();
            boolean intersects =
                    !(e.x1 < qXmin || e.x0 > qXmax || e.y1 < qYmin || e.y0 > qYmax);
            if (intersects) {
                mbrq.add(e);                        // emit THIS cell (every level)
                if (e.seq.length < index.g) {
                    for (int q = 0; q < 4; q++) {
                        double[] cb = childBounds(e.x0, e.y0, e.x1, e.y1, q);
                        int[] cs = new int[e.seq.length + 1];
                        System.arraycopy(e.seq, 0, cs, 0, e.seq.length);
                        cs[e.seq.length] = q;
                        double md = minDistToRect(cb[0], cb[1], cb[2], cb[3], trajectoryQuery);
                        stack.push(new CellEntry(cs, cb[0], cb[1], cb[2], cb[3], md));
                    }
                }
            }
        }
    }

    private static void seedAllLevelsDistanceZero(
            PriorityQueue<CellEntry> mbrq, Set<String> seen, XZ2Coder index,
            double minLon, double minLat, double maxLon, double maxLat,
            double qXmin, double qYmin, double qXmax, double qYmax,
            SpatialPoint[] trajectoryQuery) {
        Deque<CellEntry> stack = new ArrayDeque<CellEntry>();
        stack.push(new CellEntry(new int[0], minLon, minLat, maxLon, maxLat,
                minDistToRect(minLon, minLat, maxLon, maxLat, trajectoryQuery)));
        while (!stack.isEmpty()) {
            CellEntry e = stack.pop();
            // intersect test against the query MBR (distance 0 <=> intersects)
            boolean intersects =
                    !(e.x1 < qXmin || e.x0 > qXmax || e.y1 < qYmin || e.y0 > qYmax);
            if (!intersects) {
                continue;
            }
            String key = Arrays.toString(e.seq);
            if (seen.add(key)) {          // add returns false if already present
                mbrq.add(e);              // seed this distance-0 cell
            }
            if (e.seq.length < index.g) {
                for (int q = 0; q < 4; q++) {
                    double[] cb = childBounds(e.x0, e.y0, e.x1, e.y1, q);
                    int[] cs = new int[e.seq.length + 1];
                    System.arraycopy(e.seq, 0, cs, 0, e.seq.length);
                    cs[e.seq.length] = q;
                    double md = minDistToRect(cb[0], cb[1], cb[2], cb[3], trajectoryQuery);
                    stack.push(new CellEntry(cs, cb[0], cb[1], cb[2], cb[3], md));
                }
            }
        }
    }

    public static final class Result {
        public final String objectId;
        public final SpatialPoint[] trajectory;
        public final double distance;

        Result(String objectId, SpatialPoint[] trajectory, double distance) {
            this.objectId = objectId;
            this.trajectory = trajectory;
            this.distance = distance;
        }
    }


}
