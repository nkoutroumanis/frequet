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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

public class Frechet2DQueries {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries-vre.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final double epsilon = dataLoading.getDouble("epsilon");

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

        Job jobMeta = Job.getInstance();
        Job jobSegmentPoints = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(jobMeta, VRERecordMetadataReadSupport.class);
        ParquetInputFormat.setReadSupportClass(jobSegmentPoints, VRERecordSegmentPointsReadSupport.class);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Similarity Querying in VRE");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory", "4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        //determine ID
        XZ2Coder cfg = new XZ2Coder(maxResolution, minLon, maxLon, minLat, maxLat);
        SignatureCoder signatureCoder = new SignatureCoder(m, n);

        String fullPathExportedFile = metricsPath+ File.separator+"frechet-queries-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tQuery Points\tNum of Trajectories\tNum of Points\tIssued\tData Pages\tIntersected Spaces\tParse\n");
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

            long w = 0;

            double[] qMbr = new double[]{mbrMinLongitude, mbrMinLatitude, mbrMaxLongitude, mbrMaxLatitude};
            byte[] querySignature = signatureCoder.signature(trajectoryQuery, mbrMinLongitude, mbrMinLatitude, mbrMaxLongitude, mbrMaxLatitude);

            List<long[]> ranges = cfg.queryRanges(Math.max(mbrMinLongitude-epsilon,minLon+0.0000001), Math.max(mbrMinLatitude-epsilon,minLat+0.0000001), Math.min(mbrMaxLongitude+epsilon,maxLon-0.0000001), Math.min(mbrMaxLatitude+epsilon,maxLat-0.0000001),1000);
            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            Set<Long> singles = new HashSet<Long>();
            FilterPredicate keyPred = null;


            for (long[] r : ranges) {
                if (r[1] - r[0] == 1L) {
                    singles.add(r[0]);
                } else {
                    FilterPredicate p = and(gtEq(longColumn("key"), r[0]), lt(longColumn("key"), r[1]));
                    keyPred = (keyPred == null) ? p : or(keyPred, p);
                    w = w + r[1] - r[0] - 1 ;
                }
            }
            if (!singles.isEmpty()) {
                FilterPredicate inPred = in(longColumn("key"), singles);
                keyPred = (keyPred == null) ? inPred : or(keyPred, inPred);
                w = w + singles.size() ;
            }
            FilterPredicate mbr = and(and( gtEq(doubleColumn("minLongitude"), mbrMinLongitude-epsilon), gtEq(doubleColumn("minLatitude"), mbrMinLatitude-epsilon) ), and(ltEq(doubleColumn("maxLongitude"), mbrMaxLongitude+epsilon), ltEq(doubleColumn("maxLatitude"), mbrMaxLatitude+epsilon)));

            keyPred = and(keyPred, mbr);

            ParquetInputFormat.setFilterPredicate(jobMeta.getConfiguration(), keyPred);
            JavaPairRDD<Void, VRERecord> candidates = (JavaPairRDD<Void, VRERecord>) jsc.newAPIHadoopFile(parquetPath + File.separator + "sIndex", ParquetInputFormat.class, Void.class, VRERecord.class, jobMeta.getConfiguration());
            List<String> objectIds = candidates.mapToPair(f-> Tuple2.apply(f._2.getObjectId(), f._2)).groupByKey(Integer.parseInt(args[0]))
                    .mapToPair(f->{
                        int count = 0;
                        for (VRERecord vreRecord : f._2) {
                            count++;
                        }
                        List<VRERecord> records = new ArrayList<>(count);
                        f._2.forEach(records::add);
                        records.sort(Comparator.comparingLong(VRERecord::getSerialNumber));

                        if(!isFull(records)){return Tuple2.apply(false, f._1);}

                        if(pruneByLowerBound(trajectoryQuery,qMbr,querySignature,records, m, n, epsilon)){return Tuple2.apply(false, f._1);}

                        return Tuple2.apply(true, f._1);
                    }).filter(f->f._1).map(f->f._2).collect();

            Set<Binary> ids = new HashSet<>(objectIds.size());
            objectIds.forEach(f->ids.add(Binary.fromString(f)));

            if(ids.isEmpty()){
                long endTime = System.currentTimeMillis();
                bw.write((endTime-startTime)+"\t"+trajectoryQuery.length+"\t"+0+"\t"+0+"\t"+"false"+"\t"+DataPage.counter+"\t"+0+"\t"+parseAndCubeIndex);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }

            ParquetInputFormat.setFilterPredicate(jobSegmentPoints.getConfiguration(), in(binaryColumn("objectId"), ids));
            JavaPairRDD<Void, VRERecord> wholeSegments = (JavaPairRDD<Void, VRERecord>) jsc.newAPIHadoopFile(parquetPath + File.separator + "sIndex", ParquetInputFormat.class, Void.class, VRERecord.class, jobSegmentPoints.getConfiguration());

            JavaRDD<VRERecord> pairRDDRangeQuery = (JavaRDD<VRERecord>) wholeSegments.mapToPair(f-> Tuple2.apply(f._2.getObjectId(), f._2)).groupByKey(Integer.parseInt(args[0])).mapToPair(f->{
                int count = 0;
                for (VRERecord vreRecord : f._2) {
                    count++;
                }
                List<VRERecord> records = new ArrayList<>(count);
                f._2.forEach(records::add);
                records.sort(Comparator.comparingLong(VRERecord::getSerialNumber));

                VRERecord fullTrajectory = new VRERecord(f._1, records);

                double minLongitude = Double.MAX_VALUE;
                double minLatitude = Double.MAX_VALUE;
                double maxLongitude = -Double.MIN_VALUE;
                double maxLatitude = -Double.MIN_VALUE;
                for (SpatialPoint spatialPoint : fullTrajectory.getSpatialPoints()) {
                    minLongitude = Math.min(minLongitude, spatialPoint.getLongitude());
                    minLatitude = Math.min(minLatitude, spatialPoint.getLatitude());
                    maxLongitude = Math.max(maxLongitude, spatialPoint.getLongitude());
                    maxLatitude = Math.max(maxLatitude, spatialPoint.getLatitude());
                }
                SpatialPoint[] trajectoryPivots = pivots(fullTrajectory.getSpatialPoints(), minLongitude, minLatitude, maxLongitude, maxLatitude);

                for (SpatialPoint tj : trajectoryPivots) {
                    double minOverSegs = Double.POSITIVE_INFINITY;
                    minOverSegs = Math.min(minOverSegs, pointRectDist(tj.getLongitude(), tj.getLatitude(), qMbr[0], qMbr[1], qMbr[2], qMbr[3]));
                    if (minOverSegs > epsilon) {
                        return Tuple2.apply(false, fullTrajectory);
                    }
                }

                return Tuple2.apply(true, fullTrajectory);
            }).filter(f->f._1).mapToPair(f -> Tuple2.apply(HilbertUtil.frechetDistanceIsLessThanEpsilon(trajectoryQuery, f._2.getSpatialPoints(), epsilon), f._2)).filter(f -> f._1).map(f->f._2);

            List<VRERecord> trajs = pairRDDRangeQuery.collect();
            long endTime = System.currentTimeMillis();
            long num = trajs.size();

            long numOfPoints = 0;
            for (VRERecord voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2.getSpatialPoints().length;
            }

            bw.write((endTime - startTime)+"\t"+trajectoryQuery.length+"\t"+num+"\t"+numOfPoints+"\t"+"true"+"\t"+DataPage.counter+"\t"+w+"\t"+parseAndCubeIndex);
            DataPage.counter = 0;
            bw.newLine();
        }
        bw.close();
        br.close();

        String applicationId = sparkSession.sparkContext().applicationId();

        sparkSession.close();

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

            List<Long>[] lists = SparkLogParser.getTimeFromTwoStagesPerJob(eventLogFile.getAbsolutePath());
            try {
                SparkLogParser.enrichQueryAdHocFile(fullPathExportedFile, lists);
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

//    // Parse a double directly from char[] between start (inclusive) and end (exclusive)
//    private static double parseDouble(char[] chars, int start, int end) {
//        double result = 0;
//        boolean neg = false;
//        int i = start;
//        if (chars[i] == '-') { neg = true; i++; }
//
//        // Integer part
//        while (i < end && chars[i] >= '0' && chars[i] <= '9') {
//            result = result * 10 + (chars[i] - '0');
//            i++;
//        }
//
//        // Fractional part
//        if (i < end && chars[i] == '.') {
//            i++;
//            double frac = 0;
//            double div = 1;
//            while (i < end && chars[i] >= '0' && chars[i] <= '9') {
//                frac = frac * 10 + (chars[i] - '0');
//                div *= 10;
//                i++;
//            }
//            result += frac / div;
//        }
//
//        return neg ? -result : result;
//    }
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

    private static boolean isFull(List<VRERecord> group) {
        if (group.isEmpty()) {
            return false;
        }
        // group is sorted by serialNumber ascending; serials start at 1.
        boolean hasEnd = false;
        int expected = 1;                       // serials must be 1,2,3,... with no gaps
        for (VRERecord s : group) {
            if (s.getSegmentType() == 3) {      // full, unsplit trajectory
                return true;
            }
            if (s.getSerialNumber() != expected) {
                return false;                   // gap (or doesn't start at 1) => not complete
            }
            expected++;
            if (s.getSegmentType() == 2) {      // end segment present
                hasEnd = true;
            }
        }
        return hasEnd;
    }

    private static boolean pruneByLowerBound(SpatialPoint[] q, double[] qMbr, byte[] querySignature, List<VRERecord> group, int m, int n, double epsilon) {
        // LB_SES
        VRERecord start = null, end = null, full = null;
        for (VRERecord s : group) {
            int t = s.getSegmentType();
            if (t == 0) start = s;
            else if (t == 2) end = s;
            else if (t == 3) full = s;
        }
        if (full != null) { start = full; end = full; }
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


        //LB_PartialSim
        for (VRERecord s : group) {
            SpatialPoint segStart = s.getFirstPoint();
            SpatialPoint segEnd = s.getLastPoint();
            double dStart = Double.POSITIVE_INFINITY;
            double dEnd = Double.POSITIVE_INFINITY;
            for (SpatialPoint qj : q) {
                dStart = Math.min(dStart, pointDistance(qj, segStart));
                dEnd = Math.min(dEnd, pointDistance(qj, segEnd));
            }
            double segVal = Math.max(dStart, dEnd);
            if (segVal >= epsilon) {
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
//        for (int idx = 0; idx < m * n; idx++) {
//            boolean set = (querySignature[idx >> 3] & (1 << (idx & 7))) != 0;
//            if (!set) {
//                continue;
//            }
//
//            int col = idx % n;
//            int row = idx / n;
//
//            double cellW = (qMbr[2] - qMbr[0]) / n;
//            double cellH = (qMbr[3] - qMbr[1]) / m;
//
//            double rXmin = qMbr[0] + col * cellW;
//            double rYmin = qMbr[1] + row * cellH;
//            double rXmax = rXmin + cellW;
//            double rYmax = rYmin + cellH;
//
//            double distance = Double.POSITIVE_INFINITY;
//
//            for (VRERecord s : group) {
//                for (int segIdx = 0; segIdx < m * n; segIdx++) {
//                    boolean segSet = (s.getSignature()[segIdx >> 3] & (1 << (segIdx & 7))) != 0;
//                    if (!segSet) {
//                        continue;
//                    }
//
//                    int segCol = segIdx % n;
//                    int segRow = segIdx / n;
//
//                    double segCellW = (s.getMaxLongitude() - s.getMinLongitude()) / n;   // width  of one region
//                    double segCellH = (s.getMaxLatitude() - s.getMinLatitude()) / m;   // height of one region
//
//                    double segXmin = s.getMinLongitude() + segCol * segCellW;
//                    double segYmin = s.getMinLatitude() + segRow * segCellH;
//                    double segXmax = segXmin + segCellW;
//                    double segYmax = segYmin + segCellH;
//
//                    distance = Math.min(distance, rectDist(rXmin, rYmin, rXmax, rYmax, segXmin, segYmin, segXmax, segYmax));
//                }
//            }
//            if (distance>epsilon){return true;}
//        }

        return false;
    }

    public static double pointDistance(SpatialPoint a, SpatialPoint b) {
        double dx = a.getLongitude() - b.getLongitude();
        double dy = a.getLatitude() - b.getLatitude();
        return Math.sqrt(dx * dx + dy * dy);
    }

    private static double rectDist(double xmin1, double ymin1, double xmax1, double ymax1, double xmin2, double ymin2, double xmax2, double ymax2) {
        double dx = Math.max(Math.max(xmin1 - xmax2, xmin2 - xmax1), 0.0);
        double dy = Math.max(Math.max(ymin1 - ymax2, ymin2 - ymax1), 0.0);
        return Math.sqrt(dx * dx + dy * dy);
    }

    private static double pointRectDist(double px,double py, double x0,double y0,double x1,double y1){
        double dx = px<x0?x0-px:(px>x1?px-x1:0.0);
        double dy = py<y0?y0-py:(py>y1?py-y1:0.0);
        return Math.sqrt(dx*dx+dy*dy);
    }

}
