package tman;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecord;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecordReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import tman.impl.DPFeatureExtractor;
import tman.impl.InMemorySignatureCache;
import tman.impl.XZConfig;
import tman.impl.XZTrajectoryIndex;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static tman.DpUtils.boxToBoxSetDistance;
import static tman.DpUtils.distanceToNearestBox;

public class KnnQueries {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries-tman.conf");

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
                ).resolve().getConfig("tman");
            }
        } else {
            metadata = ConfigFactory.parseFile(new File(parquetPath + File.separator + "space.metadata")).resolve().getConfig("tman");
        }

        Config boundaries = metadata.getConfig("boundaries");
        final double minLon = boundaries.getDouble("minLon");
        final double minLat = boundaries.getDouble("minLat");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");

        final int maxResolution = metadata.getInt("maxResolution");
        final int alpha = metadata.getInt("alpha");
        final int beta = metadata.getInt("beta");

        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, TManRecordReadSupport.class);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Similarity Querying in TMan");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory", "4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        //determine ID
        XZConfig cfg = new XZConfig(maxResolution,  alpha, beta, minLon, maxLon, minLat, maxLat);
        XZTrajectoryIndex index = new XZTrajectoryIndex(cfg);

        Map<Long, HashSet<Long>> codeShapes;
        if (parquetPath.startsWith("hdfs://")) {
            FileSystem fs = new Path(parquetPath).getFileSystem(job.getConfiguration());
            try (FSDataInputStream hdfsIn = fs.open(new Path(parquetPath+File.separator+"cache.ser"));
                 ObjectInputStream in = new ObjectInputStream(hdfsIn)) {
                codeShapes = (Map<Long, HashSet<Long>>) in.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }else{
            try (ObjectInputStream in = new ObjectInputStream(
                    new FileInputStream(parquetPath+File.separator+"cache.ser"))) {
                codeShapes = (Map<Long, HashSet<Long>>) in.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        InMemorySignatureCache cache = InMemorySignatureCache.fromMap(codeShapes, cfg);

        String fullPathExportedFile = metricsPath+ File.separator+"knn-"+k+"-queries-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tQuery Points\tNum of Trajectories\tk-th Distance\tNum of Points\tData Pages\tParse\tQuery rounds\n");

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

            int rounds=0;
//            List<Double> varEpsilon  = new ArrayList<>();
//            List<Double> varMinDist = new ArrayList<>();
            DPFeatureExtractor.DPFeatures d = DPFeatureExtractor.extract(trajectoryQuery);
            int[] dpPointIndexesQuery = d.getPointIndexes();
            DPFeatureExtractor.BoundingBox[] dpMbrsQuery = d.getBoundingBoxes();

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;
            List<Set<Long>> myset = new ArrayList<>();

            PriorityQueue<CellEntry> EQ = new PriorityQueue<CellEntry>((a, b) -> Double.compare(a.minDistEE, b.minDistEE));
            PriorityQueue<ISEntry> IQ = new PriorityQueue<ISEntry>((a, b) -> Double.compare(a.minDistIS, b.minDistIS));
            PriorityQueue<Result> result = new PriorityQueue<Result>((a, b) -> Double.compare(b.distance, a.distance));

            double epsilon = Double.POSITIVE_INFINITY;

            // query envelope (raw coords), for the containment prune
            double qXmin = Double.POSITIVE_INFINITY, qYmin = Double.POSITIVE_INFINITY;
            double qXmax = Double.NEGATIVE_INFINITY, qYmax = Double.NEGATIVE_INFINITY;
            for (SpatialPoint p : trajectoryQuery) {
                qXmin = Math.min(qXmin, p.getLongitude()); qYmin = Math.min(qYmin, p.getLatitude());
                qXmax = Math.max(qXmax, p.getLongitude()); qYmax = Math.max(qYmax, p.getLatitude());
            }

            EQ.add(new CellEntry(new int[0], minLon, minLat, maxLon, maxLat,
                    minDist(minLon, minLat, maxLon, maxLat, trajectoryQuery)));

            while (!EQ.isEmpty() || !IQ.isEmpty()) {
                double eqKey = EQ.isEmpty() ? Double.POSITIVE_INFINITY : EQ.peek().minDistEE;
                double iqKey = IQ.isEmpty() ? Double.POSITIVE_INFINITY : IQ.peek().minDistIS;

                if (result.size() >= k && Math.min(eqKey, iqKey) >= epsilon) {
                    break;
                }

                if (iqKey <= eqKey) {
                    // ---- equal-distance batch: one Spark job for all index spaces at this distance
                    Set<Long> batchKeys = new HashSet<>();
                    double d0 = IQ.peek().minDistIS;
                    while (!IQ.isEmpty() && IQ.peek().minDistIS == d0) {
                        ISEntry is = IQ.peek();
                        if (is.minDistIS >= epsilon && result.size() >= k) { IQ.poll(); continue; }
                        IQ.poll();
                        batchKeys.add(index.tShape(is.code, is.signature));
                    }
                    if (batchKeys.isEmpty()) continue;


                    rounds++;
                    final double finalEpsilon = epsilon;

                    ParquetInputFormat.setFilterPredicate(job.getConfiguration(), in(longColumn("key"), batchKeys));

                    myset.add(batchKeys);

                    JavaPairRDD<Void, TManRecord> candidates =
                            (JavaPairRDD<Void, TManRecord>) jsc.newAPIHadoopFile(parquetPath + File.separator + "sIndex", ParquetInputFormat.class, Void.class, TManRecord.class, job.getConfiguration());
                    JavaPairRDD<Double, TManRecord> fetchedTrajectories = candidates.filter(f -> {
                                SpatialPoint[] spatialPoints = f._2.getSpatialPoints();
                                int[] dpPointIndexesTrajectory = f._2.getDpPointIndexes();
                                DPFeatureExtractor.BoundingBox[] dpMbrsTrajectory = f._2.getDpMbrs();
                                if (Double.compare(HilbertUtil.euclideanDistance(
                                                spatialPoints[0].getLongitude(), spatialPoints[0].getLatitude(),
                                                trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()),
                                        finalEpsilon) == 1) return false;
                                if (Double.compare(HilbertUtil.euclideanDistance(
                                                spatialPoints[spatialPoints.length - 1].getLongitude(),
                                                spatialPoints[spatialPoints.length - 1].getLatitude(),
                                                trajectoryQuery[trajectoryQuery.length - 1].getLongitude(),
                                                trajectoryQuery[trajectoryQuery.length - 1].getLatitude()),
                                        finalEpsilon) == 1) return false;
                                for (int i : dpPointIndexesQuery)
                                    if (distanceToNearestBox(trajectoryQuery[i], dpMbrsTrajectory) > finalEpsilon) return false;
                                for (int i : dpPointIndexesTrajectory)
                                    if (distanceToNearestBox(spatialPoints[i], dpMbrsQuery) > finalEpsilon) return false;
                                for (DPFeatureExtractor.BoundingBox box : dpMbrsQuery)
                                    if (boxToBoxSetDistance(box, dpMbrsTrajectory) > finalEpsilon) return false;
                                for (DPFeatureExtractor.BoundingBox box : dpMbrsTrajectory)
                                    if (boxToBoxSetDistance(box, dpMbrsQuery) > finalEpsilon) return false;
                                return true;
                            })
                            .mapToPair(f -> Tuple2.apply(HilbertUtil.frechetDistance(trajectoryQuery, f._2.getSpatialPoints()), f._2)).filter(f -> f._1 <= finalEpsilon);
                    for (Tuple2<Double, TManRecord> res : fetchedTrajectories.collect()) {
                        if (result.size() < k) result.add(new Result(res._2, res._1));
                        else if (res._1 < result.peek().distance) { result.poll(); result.add(new Result(res._2, res._1)); }
                    }
                    epsilon = EPS(result, k);
//                    varEpsilon.add(epsilon);
                } else {
                    // ---- expand cell, with TraSS containment prune
                    CellEntry e = EQ.poll();
                    if (e.minDistEE >= epsilon && result.size() >= k) continue;

                    // TraSS neededToCheck: enlarged element (doubled toward upper-right) +
                    // dilated by epsilon must CONTAIN the query envelope, else prune.
                    // Sound: monotone in epsilon; if it fails now it fails for any smaller eps.
                    boolean keep = true;
                    if (result.size() >= k && epsilon != Double.POSITIVE_INFINITY) {
                        double xLen = e.x1 - e.x0, yLen = e.y1 - e.y0;
                        double ex0 = e.x0 - epsilon,          ey0 = e.y0 - epsilon;
                        double ex1 = (e.x1 + xLen) + epsilon, ey1 = (e.y1 + yLen) + epsilon;
                        keep = ex0 <= qXmin && ey0 <= qYmin && ex1 >= qXmax && ey1 >= qYmax;
                    }
                    if (!keep) continue;

                    long code = index.quadrantCode(e.seq);
                    long[] sigs = cache.signaturesForCell(code);
                    if (sigs != null) {
                        for (long s : sigs) {
                            double mdis = minDistIS(e.x0, e.y0, e.x1, e.y1, s, trajectoryQuery, cfg);
                            IQ.add(new ISEntry(code, s, mdis));
                        }
                    }
                    if (e.seq.length < cfg.g) {
                        for (int q = 0; q < 4; q++) {
                            double[] cb = childBounds(e.x0, e.y0, e.x1, e.y1, q);
                            int[] cs = new int[e.seq.length + 1];
                            System.arraycopy(e.seq, 0, cs, 0, e.seq.length);
                            cs[e.seq.length] = q;
                            double mdee = minDist(cb[0], cb[1], cb[2], cb[3], trajectoryQuery);
                            EQ.add(new CellEntry(cs, cb[0], cb[1], cb[2], cb[3], mdee));
                        }
                    }
                }
            }

//            for (Result result1 : drain(result)) {
//                System.out.println(result1.tmanRecord);
//            }
//
//            System.out.println(rounds);
//            System.out.println(varEpsilon);
//            System.out.println(varMinDist);
//            System.out.println(myset);

//            List<Tuple2<Void,TManRecord>> trajs = pairRDDRangeQuery.collect();
            long endTime = System.currentTimeMillis();

            long numOfPoints = 0;
            for (Result voidTrajectoryTuple2 : result) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2.tmanRecord.getSpatialPoints().length;
            }

            bw.write((endTime - startTime)+"\t"+trajectoryQuery.length+"\t"+result.size()+"\t"+result.peek().distance+"\t"+numOfPoints+"\t"+DataPage.counter+"\t"+parseAndCubeIndex+"\t"+rounds);
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

            List<Long>[] lists = SparkLogParser.getMetricsAndTimeStagesPerJob(eventLogFile.getAbsolutePath());
            try {
                SparkLogParser.enrichQueryAdHocFileWithMetricsAndTimeStages(fullPathExportedFile, lists);
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

    public static final class Result {
        public final TManRecord tmanRecord;
        public final double distance;
        public Result(TManRecord tmanRecord, double d) { this.tmanRecord = tmanRecord; this.distance = d; }
    }

    private static final class CellEntry {
        final int[] seq; final double x0, y0, x1, y1; final double minDistEE;
        CellEntry(int[] seq, double x0, double y0, double x1, double y1, double m) {
            this.seq=seq; this.x0=x0; this.y0=y0; this.x1=x1; this.y1=y1; this.minDistEE=m;
        }
    }
    private static final class ISEntry {
        final long code; final long signature; final double minDistIS;
        ISEntry(long c, long s, double m){ this.code=c; this.signature=s; this.minDistIS=m; }
    }

    private static double pointRectDist(double px,double py,double x0,double y0,double x1,double y1){
        double dx = px<x0?x0-px:(px>x1?px-x1:0.0);
        double dy = py<y0?y0-py:(py>y1?py-y1:0.0);
        return Math.sqrt(dx*dx+dy*dy);
    }
    private static double minDist(double x0,double y0,double x1,double y1, SpatialPoint[] qn){
        double best=Double.POSITIVE_INFINITY;
        for(SpatialPoint p:qn){ double d=pointRectDist(p.getLongitude(),p.getLatitude(),x0,y0,x1,y1); if(d<best)best=d; if(best==0)break; }
        return best;
    }
    private static double minDistIS(double cx0,double cy0,double cx1,double cy1,long sig, SpatialPoint[] qn, XZConfig cfg){
        double cw=(cx1-cx0)/cfg.beta, ch=(cy1-cy0)/cfg.alpha, best=Double.POSITIVE_INFINITY;
        long bits=sig;
        while(bits!=0){ int b=Long.numberOfTrailingZeros(bits); bits&=(bits-1);
            int row=b/cfg.beta, col=b%cfg.beta;
            double sx0=cx0+col*cw, sy0=cy0+row*ch;
            double d=minDist(sx0,sy0,sx0+cw,sy0+ch,qn); if(d<best)best=d; if(best==0)break; }
        return best;
    }
    private static double[] childBounds(double x0,double y0,double x1,double y1,int q){
        double mx=(x0+x1)/2.0,my=(y0+y1)/2.0; boolean r=(q&1)==1,t=(q&2)==2;
        return new double[]{ r?mx:x0, t?my:y0, r?x1:mx, t?y1:my };
    }

    private static List<Result> drain(PriorityQueue<Result> pq){
        List<Result> out=new ArrayList<Result>(pq);
        out.sort((a,b)->Double.compare(a.distance,b.distance));
        return out;
    }


    /** Current epsilon: k-th best distance once k are held, else +infinity. */
    private static double EPS(PriorityQueue<Result> result, int k) {
        return result.size() >= k ? result.peek().distance : Double.POSITIVE_INFINITY;
    }

}
