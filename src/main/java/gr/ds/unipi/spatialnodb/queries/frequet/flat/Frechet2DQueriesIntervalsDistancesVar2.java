package gr.ds.unipi.spatialnodb.queries.frequet.flat;

import com.typesafe.config.Config;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithIntervalMetadata;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithIntervalMetadataReadSupport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Frechet2DQueriesIntervalsDistancesVar2 {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final double epsilon = dataLoading.getDouble("epsilon");

        Job job = Job.getInstance();
        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentWithIntervalMetadataReadSupport.class);

        SparkConf sparkConf = new SparkConf();//.registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class});/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
        sparkConf.setAppName("Similarity Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory", "4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        String fullPathExportedFile = metricsPath+ File.separator+"frechet-queries-distances-var2-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tQuery Points\tNum of Trajectories\tNum of Points\tIssued\tData Pages\tParse\n");
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

//            final double queryMinLongitude = mbrMinLongitude-epsilon;
//            final double queryMinLatitude =mbrMinLatitude-epsilon;
//
//            final double queryMaxLongitude = mbrMaxLongitude+epsilon;
//            final double queryMaxLatitude = mbrMaxLatitude+epsilon;


            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), trajectoryQuery[0].getLongitude()-epsilon), ltEq(doubleColumn("minLongitude"), trajectoryQuery[0].getLongitude()+epsilon));
            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), trajectoryQuery[0].getLatitude()-epsilon), ltEq(doubleColumn("minLatitude"), trajectoryQuery[0].getLatitude()+epsilon));
            FilterPredicate fp = and(xAxis, yAxis);

            for (int i = 1; i < trajectoryQuery.length; i++) {
                xAxis = and(gtEq(doubleColumn("maxLongitude"), trajectoryQuery[i].getLongitude()-epsilon), ltEq(doubleColumn("minLongitude"), trajectoryQuery[i].getLongitude()+epsilon));
                yAxis = and(gtEq(doubleColumn("maxLatitude"), trajectoryQuery[i].getLatitude()-epsilon), ltEq(doubleColumn("minLatitude"), trajectoryQuery[i].getLatitude()+epsilon));
                fp = or(fp, and(xAxis, yAxis));
            }

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), fp);

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            JavaPairRDD<Void, TrajectorySegmentWithIntervalMetadata> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegmentWithIntervalMetadata>) jsc.newAPIHadoopFile(parquetPath+ File.separator+"stIndex"+File.separator, ParquetInputFormat.class, Void.class, TrajectorySegmentWithIntervalMetadata.class, job.getConfiguration());


            pairRDDRangeQuery = pairRDDRangeQuery.filter(f -> {
                SpatialPoint[] spatialPoints = f._2().getTrajectorySegment().getSpatialPoints();

                if(f._2.getInterval()[0]==1){
                    if(Double.compare(HilbertUtil.euclideanDistance(spatialPoints[0].getLongitude(),spatialPoints[0].getLatitude(),trajectoryQuery[0].getLongitude(),trajectoryQuery[0].getLatitude()),epsilon)==1){
                        return false;
                    }
                }

                if(f._2.getInterval()[1]<0){
                    if(Double.compare(HilbertUtil.euclideanDistance(spatialPoints[spatialPoints.length-1].getLongitude(),spatialPoints[spatialPoints.length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude()),epsilon)==1){
                        return false;
                    }
                }

                //All distances pruning
                for (SpatialPoint spatialPoint : spatialPoints) {
                    if(HilbertUtil.isPointMinDistGreaterThan(spatialPoint.getLongitude(), spatialPoint.getLatitude(), trajectoryQuery, epsilon)){
                        return false;
                    }
                }

                return true;
            });



            JavaPairRDD<Void, TrajectorySegment> results = pairRDDRangeQuery.groupBy(f->f._2().getTrajectorySegment().getObjectId(), Integer.parseInt(args[0]))
                    .flatMapToPair(f->{

                        List<TrajectorySegmentWithIntervalMetadata> trSegments = new ArrayList<>();
                        f._2.forEach(t->trSegments.add(t._2));

                        Comparator<TrajectorySegmentWithIntervalMetadata> comparator = Comparator.comparingLong(d-> d.getInterval()[0]);
                        trSegments.sort(comparator);

                        if(trSegments.size()==1 && trSegments.get(0).getInterval()[0]==1 && trSegments.get(0).getInterval()[1]<0){
                            return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, trSegments.get(0).getTrajectorySegment())).iterator();
                        }

                        long y;
                        if(trSegments.get(0).getInterval()[0]!=1 || trSegments.get(trSegments.size()-1).getInterval()[1]>0){
                            return Collections.emptyIterator();
                        }else{
                            y = trSegments.get(0).getInterval()[1];
                        }
                        for (int i = 1; i < trSegments.size()-1; i++) {
                                if(y+1 != trSegments.get(i).getInterval()[0]) {return Collections.emptyIterator();}
                                y = trSegments.get(i).getInterval()[1];
                        }
                        if(y+1!=trSegments.get(trSegments.size()-1).getInterval()[0]){return Collections.emptyIterator();}

                        List<TrajectorySegment> ts = new ArrayList<>(trSegments.size());
                        trSegments.forEach(e->ts.add(e.getTrajectorySegment()));

                        return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, new TrajectorySegment(f._1, ts))).iterator();
                    }).filter(f-> HilbertUtil.frechetDistanceIsLessThanEpsilon(trajectoryQuery, f._2.getSpatialPoints(),epsilon));

            List<Tuple2<Void,TrajectorySegment>> trajs = results.collect();
            long endTime = System.currentTimeMillis();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatialPoints().length;
            }

            bw.write((endTime - startTime)+"\t"+trajectoryQuery.length+"\t"+num+"\t"+numOfPoints+"\t"+"true"+"\t"+DataPage.counter+"\t"+"\t"+parseAndCubeIndex);
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
