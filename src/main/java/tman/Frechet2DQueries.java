package tman;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecord;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecordReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FSDataInputStream;
import scala.Tuple2;
import tman.impl.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static tman.DpUtils.boxToBoxSetDistance;
import static tman.DpUtils.distanceToNearestBox;

public class Frechet2DQueries {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries-tman.conf");

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

//        codeShapes.forEach((k,v)->{System.out.println(k +" "+v);});
        InMemorySignatureCache cache = InMemorySignatureCache.fromMap(codeShapes, cfg);


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

            FrechetCachedQuery fcq = new FrechetCachedQuery(cfg, index, cache);
            List<LongRange> ranges = fcq.queryRanges(trajectoryQuery,epsilon);
            HashSet<Long> singleValues = new HashSet<>();
            FilterPredicate fp = null;
            if(!ranges.isEmpty()){
                fp = and(gtEq(longColumn("key"), ranges.get(0).lo), lt(longColumn("key"), ranges.get(0).hi));
            }
            w = w + (ranges.get(0).hi-ranges.get(0).lo);

            for (int i = 1; i < ranges.size(); i++) {
                if(ranges.get(i).hi-ranges.get(i).lo==1){
                    singleValues.add(ranges.get(i).lo);
                    w++;
                }else{
                    fp = or(and(gtEq(longColumn("key"), ranges.get(i).lo), lt(longColumn("key"), ranges.get(i).hi)),fp);
                    w = w + (ranges.get(i).hi-ranges.get(i).lo);
                }
            }

            if(singleValues.size()!=1){
                fp = or(fp, in(longColumn("key"), singleValues));
            }

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), fp);

            DPFeatureExtractor.DPFeatures d = DPFeatureExtractor.extract(trajectoryQuery);
            int[] dpPointIndexesQuery = d.getPointIndexes();
            DPFeatureExtractor.BoundingBox[] dpMbrsQuery = d.getBoundingBoxes();

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            if(ranges.isEmpty()){
                long endTime = System.currentTimeMillis();
                bw.write((endTime-startTime)+"\t"+trajectoryQuery.length+"\t"+0+"\t"+0+"\t"+"false"+"\t"+DataPage.counter+"\t"+0+"\t"+parseAndCubeIndex);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }

            JavaPairRDD<Void, TManRecord> pairRDDRangeQuery = (JavaPairRDD<Void, TManRecord>) jsc.newAPIHadoopFile(parquetPath+ File.separator+"sIndex", ParquetInputFormat.class, Void.class, TManRecord.class, job.getConfiguration());
            pairRDDRangeQuery = pairRDDRangeQuery.filter(f->{
                SpatialPoint[] spatialPoints = f._2().getSpatialPoints();

                int[] dpPointIndexesTrajectory = f._2.getDpPointIndexes();
                DPFeatureExtractor.BoundingBox[] dpMbrsTrajectory = f._2.getDpMbrs();


                if(Double.compare(HilbertUtil.euclideanDistance(spatialPoints[0].getLongitude(),spatialPoints[0].getLatitude(),trajectoryQuery[0].getLongitude(),trajectoryQuery[0].getLatitude()),epsilon)==1){
                    return false;
                }

                if(Double.compare(HilbertUtil.euclideanDistance(spatialPoints[spatialPoints.length-1].getLongitude(),spatialPoints[spatialPoints.length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude()),epsilon)==1){
                    return false;
                }

                for (int i : dpPointIndexesQuery) {
                    if (distanceToNearestBox(trajectoryQuery[i], dpMbrsTrajectory) > epsilon) {
                        return false;
                    }
                }
                for (int i : dpPointIndexesTrajectory) {
                    if (distanceToNearestBox(spatialPoints[i], dpMbrsQuery) > epsilon) {
                        return false;
                    }
                }

                for (DPFeatureExtractor.BoundingBox box : dpMbrsQuery) {
                    if (boxToBoxSetDistance(box, dpMbrsTrajectory) > epsilon) {
                        return false;
                    }
                }

                for (DPFeatureExtractor.BoundingBox box : dpMbrsTrajectory) {
                    if (boxToBoxSetDistance(box, dpMbrsQuery) > epsilon) {
                        return false;
                    }
                }
                return true;
            }).filter(f-> HilbertUtil.frechetDistanceIsLessThanEpsilon(trajectoryQuery, f._2.getSpatialPoints(),epsilon));
//            System.out.println(pairRDDRangeQuery.count());

            List<Tuple2<Void,TManRecord>> trajs = pairRDDRangeQuery.collect();
            long endTime = System.currentTimeMillis();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TManRecord> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatialPoints().length;
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

}
