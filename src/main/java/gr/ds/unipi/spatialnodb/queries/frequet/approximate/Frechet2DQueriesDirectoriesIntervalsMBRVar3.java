package gr.ds.unipi.spatialnodb.queries.frequet.approximate;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.shapes.Point;
import gr.ds.unipi.shapes.Rectangle;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithIntervalMetadata;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithIntervalMetadataReadSupport;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.pathReadParquet.ParquetInputFormatWithKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.minDistPointToRectangle;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Frechet2DQueriesDirectoriesIntervalsMBRVar3 {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final double epsilon = dataLoading.getDouble("epsilon");

        Config metadata;
        if(parquetPath.startsWith("hdfs://")){
            Path filePath = new Path(parquetPath, "space.metadata");
            FileSystem fs = filePath.getFileSystem(new Configuration());
            try (InputStream in = fs.open(filePath)) {
                metadata = ConfigFactory.parseReader(
                        new InputStreamReader(in)
                ).resolve().getConfig("gridHilbert");
            }
        }else{
            metadata = ConfigFactory.parseFile(new File(parquetPath+ File.separator+"space.metadata")).resolve().getConfig("gridHilbert");
        }
        final int bits = metadata.getInt("bits");
        Config boundaries = metadata.getConfig("boundaries");
        final double minLon = boundaries.getDouble("minLon");
        final double minLat = boundaries.getDouble("minLat");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        final IndexUtils indexUtils = new IndexUtils(minLon, minLat, maxLon, maxLat, maxOrdinates);

        Job job = Job.getInstance();

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentWithIntervalMetadataReadSupport.class);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Similarity Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
//        Broadcast<SmallHilbertCurve> smallHilbertCurveBr = jsc.<SmallHilbertCurve>broadcast(hilbertCurve);

        Set<Long> directoriesSet = new HashSet<>();
        if (parquetPath.startsWith("hdfs://")) {
            Path stIndexPath = new Path(parquetPath + "/stIndex");
            FileSystem fs = stIndexPath.getFileSystem(job.getConfiguration());
            FileStatus[] statuses = fs.listStatus(stIndexPath);

            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    directoriesSet.add(Long.parseLong(status.getPath().getName()));
                }
            }
        } else {
            File[] directories = new File(parquetPath + File.separator + "stIndex").listFiles(File::isDirectory);
            for (File directory : directories) {
                directoriesSet.add(Long.parseLong(directory.getName()));
            }
        }

        String fullPathExportedFile = metricsPath+ File.separator+"frechet-queries-approximate-mbr-var3-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tQuery Points\tNum of Trajectories\tNum of Points\tIssued\tData Pages\tIntersected Cubes\tParse\n");
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

//            final double queryMinLongitude = Double.max(minLon,mbrMinLongitude-epsilon);
//            final double queryMinLatitude = Double.max(minLat,mbrMinLatitude-epsilon);
//
//            final double queryMaxLongitude = Double.min(maxLon-0.0000001,mbrMaxLongitude+epsilon);
//            final double queryMaxLatitude = Double.min(maxLat-0.0000001,mbrMaxLatitude+epsilon);
//
//            Map<Long, List<IndexInterval>> queryIndexIntervals= new HashMap<>();
//            int intervalStart = 1;
//            long currentHilValue = hilbertCurve.index(indexUtils.scale(trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()));
//            for (int i = 1; i < trajectoryQuery.length; i++) {
//                long hilbertValue = hilbertCurve.index(indexUtils.scale(trajectoryQuery[i].getLongitude(), trajectoryQuery[i].getLatitude()));
//                if(currentHilValue != hilbertValue){
//                    queryIndexIntervals.computeIfAbsent(currentHilValue, k -> new ArrayList<>()).add(IndexInterval.newIndexInterval(intervalStart, i));
//                    currentHilValue = hilbertValue;
//                    intervalStart = i + 1;
//                }
//            }
//            queryIndexIntervals.computeIfAbsent(currentHilValue, k -> new ArrayList<>()).add(IndexInterval.newIndexInterval(intervalStart, trajectoryQuery.length));
//
//            List<Rectangle> rectangles = new ArrayList<>();
//            for (List<IndexInterval> indexInterval : queryIndexIntervals.values()) {
//                for (IndexInterval interval : indexInterval) {
//                    mbrMinLongitude = Double.MAX_VALUE;
//                    mbrMinLatitude= Double.MAX_VALUE;
//                    mbrMaxLongitude= -Double.MAX_VALUE;
//                    mbrMaxLatitude= -Double.MAX_VALUE;
//                    for (int i = interval.getStart()-1; i <= interval.getEnd()-1; i++) {
//                        mbrMinLongitude = Math.min(trajectoryQuery[i].getLongitude(), mbrMinLongitude);
//                        mbrMinLatitude = Math.min(trajectoryQuery[i].getLatitude(), mbrMinLatitude);
//                        mbrMaxLongitude = Math.max(trajectoryQuery[i].getLongitude(), mbrMaxLongitude);
//                        mbrMaxLatitude = Math.max(trajectoryQuery[i].getLatitude(), mbrMaxLatitude);
//                    }
//                    rectangles.add(Rectangle.newRectangle(Point.newPoint(mbrMinLongitude, mbrMinLatitude), Point.newPoint(mbrMaxLongitude, mbrMaxLatitude)));
//                }
//            }

            List<Rectangle> rectangles = new ArrayList<>();
            long currentHilValue = hilbertCurve.index(indexUtils.scale(trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()));
            mbrMinLongitude = trajectoryQuery[0].getLongitude();
            mbrMinLatitude= trajectoryQuery[0].getLatitude();
            mbrMaxLongitude= trajectoryQuery[0].getLongitude();
            mbrMaxLatitude= trajectoryQuery[0].getLatitude();

            for (int i = 1; i < trajectoryQuery.length; i++) {
                long hilbertValue = hilbertCurve.index(indexUtils.scale(trajectoryQuery[i].getLongitude(), trajectoryQuery[i].getLatitude()));
                if(currentHilValue != hilbertValue){
                    rectangles.add(Rectangle.newRectangle(Point.newPoint(mbrMinLongitude, mbrMinLatitude), Point.newPoint(mbrMaxLongitude, mbrMaxLatitude)));
                    currentHilValue = hilbertValue;
                    mbrMinLongitude = trajectoryQuery[i].getLongitude();
                    mbrMinLatitude= trajectoryQuery[i].getLatitude();
                    mbrMaxLongitude= trajectoryQuery[i].getLongitude();
                    mbrMaxLatitude= trajectoryQuery[i].getLatitude();
                }else{
                    mbrMinLongitude = Math.min(mbrMinLongitude, trajectoryQuery[i].getLongitude());
                    mbrMinLatitude = Math.min(mbrMinLatitude, trajectoryQuery[i].getLatitude());
                    mbrMaxLongitude = Math.max(mbrMaxLongitude, trajectoryQuery[i].getLongitude());
                    mbrMaxLatitude = Math.max(mbrMaxLatitude, trajectoryQuery[i].getLatitude());
                }
            }
            rectangles.add(Rectangle.newRectangle(Point.newPoint(mbrMinLongitude, mbrMinLatitude), Point.newPoint(mbrMaxLongitude, mbrMaxLatitude)));

//            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
//            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
//            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
//            StringBuilder sb = new StringBuilder();
//
//                for (Range range : ranges.toList()) {
//                    for (long r = range.low(); r <= range.high(); r++) {
//                        if(directoriesSet.contains(String.valueOf(r))) {
//                            long[] cube = hilbertCurve.point(r);
//                            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
//                            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//                            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
//                            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
//
//                            if(areTrajectoryPointsDistanceLessThanEpsilonToCube(trajectoryQuery, xMin, yMin, xMax, yMax, epsilon)){
//                                    sb.append(parquetPath+ File.separator+"stIndex"+File.separator+r+",");
//                            }
//                        }
//                    }
//                }

            Set<Long>[] mappedCells = new HashSet[trajectoryQuery.length];
            Set<Long> queryCells = new HashSet<>();
            for (int i = 0; i < trajectoryQuery.length; i++) {
                Set<Long> pointCellsSet = new HashSet<>();
                long[] hilStart = indexUtils.scale(Math.max(minLon, trajectoryQuery[i].getLongitude()-epsilon), Math.max(minLat,trajectoryQuery[i].getLatitude()-epsilon));//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
                long[] hilEnd = indexUtils.scale(Math.min(maxLon-0.0000001, trajectoryQuery[i].getLongitude()+epsilon), Math.min(maxLat-0.0000001, trajectoryQuery[i].getLatitude()+epsilon));//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
                Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);

                for (Range range : ranges.toList()) {
                    for (long r = range.low(); r <= range.high(); r++) {
                        long[] cube = hilbertCurve.point(r);
                        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

                        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

                        if(minDistPointToRectangle(trajectoryQuery[i].getLongitude(), trajectoryQuery[i].getLatitude(), xMin, yMin, xMax, yMax)<=epsilon){
//                                sb.append(parquetPath+ File.separator+"stIndex"+File.separator+r+",");
                            pointCellsSet.add(r);
                            if(directoriesSet.contains(r)) {
                                queryCells.add(r);
                            }
                        }
                    }
                }
                mappedCells[i] = pointCellsSet;
            }

            StringBuilder sb = new StringBuilder();
            for (Long queryCellsIds : queryCells) {
                sb.append(parquetPath+ File.separator+"stIndex"+File.separator+queryCellsIds+",");
            }

            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), rectangles.get(0).getLowerBound().getX()-epsilon), ltEq(doubleColumn("minLongitude"), rectangles.get(0).getUpperBound().getX()+epsilon));
            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), rectangles.get(0).getLowerBound().getY()-epsilon), ltEq(doubleColumn("minLatitude"), rectangles.get(0).getUpperBound().getY()+epsilon));
            FilterPredicate fp = and(xAxis, yAxis);

            for (int i = 1; i < rectangles.size(); i++) {
                xAxis = and(gtEq(doubleColumn("maxLongitude"), rectangles.get(i).getLowerBound().getX()-epsilon), ltEq(doubleColumn("minLongitude"), rectangles.get(i).getUpperBound().getX()+epsilon));
                yAxis = and(gtEq(doubleColumn("maxLatitude"), rectangles.get(i).getLowerBound().getY()-epsilon), ltEq(doubleColumn("minLatitude"), rectangles.get(i).getUpperBound().getY()+epsilon));
                fp = or(fp, and(xAxis, yAxis));
            }

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), fp);

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            if(sb.length()==0){
                long endTime = System.currentTimeMillis();
                bw.write((endTime-startTime)+"\t"+trajectoryQuery.length+"\t"+0+"\t"+0+"\t"+"false"+"\t"+DataPage.counter+"\t"+0+"\t"+parseAndCubeIndex);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }

            sb.deleteCharAt(sb.length()-1);
            JavaPairRDD<Long, TrajectorySegmentWithIntervalMetadata> pairRDDRangeQuery = (JavaPairRDD<Long, TrajectorySegmentWithIntervalMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormatWithKey.class, Long.class, TrajectorySegmentWithIntervalMetadata.class, job.getConfiguration());


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

                //MBR pruning
                if(HilbertUtil.isMinDistGreaterThan(f._2.getTrajectorySegment().getMinLongitude(), f._2.getTrajectorySegment().getMinLatitude(), f._2.getTrajectorySegment().getMaxLongitude(), f._2.getTrajectorySegment().getMaxLatitude(), trajectoryQuery, epsilon)){
                    return false;
                }

                return true;
            });

//                        System.out.println(pairRDDRangeQuery.collect().size());
//            System.exit(5);
//            if(pairRDDRangeQuery.collect().isEmpty()){
//                System.exit(5);
//            }


            JavaPairRDD<Void, TrajectorySegment> results = (JavaPairRDD<Void, TrajectorySegment>) pairRDDRangeQuery.groupBy(f->f._2().getTrajectorySegment().getObjectId(), Integer.parseInt(args[0]))
                    .flatMapToPair(f->{

                        List<Tuple2<Long,TrajectorySegmentWithIntervalMetadata>> trSegments = new ArrayList<>();
                        f._2.forEach(trSegments::add);

                        Comparator<Tuple2<Long, TrajectorySegmentWithIntervalMetadata>> comparator = Comparator.comparingLong(d-> d._2.getInterval()[0]);
                        trSegments.sort(comparator);

                        if(trSegments.size()==1 && trSegments.get(0)._2.getInterval()[0]==1 && trSegments.get(0)._2.getInterval()[1]<0){
                            return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, trSegments.get(0)._2.getTrajectorySegment())).iterator();
                        }

                        long y;
                        if(trSegments.get(0)._2.getInterval()[0]!=1 || trSegments.get(trSegments.size()-1)._2.getInterval()[1]>0){
                            return Collections.emptyIterator();
                        }else{
                            y = trSegments.get(0)._2.getInterval()[1];
                        }
                        for (int i = 1; i < trSegments.size()-1; i++) {
                            if(y+1 != trSegments.get(i)._2.getInterval()[0]) {return Collections.emptyIterator();}
                            y = trSegments.get(i)._2.getInterval()[1];
                        }
                        if(y+1!=trSegments.get(trSegments.size()-1)._2.getInterval()[0]){return Collections.emptyIterator();}

                        List<TrajectorySegment> ts = new ArrayList<>(trSegments.size());
                        trSegments.forEach(e->ts.add(e._2.getTrajectorySegment()));

                        //pruning
                        Set<Long> trackletsCellIds = new HashSet<>(trSegments.size());
                        f._2.forEach(t->trackletsCellIds.add(t._1));

                        for (Set<Long> mappedCell : mappedCells) {
                            if (Collections.disjoint(mappedCell, trackletsCellIds)) {
                                return Collections.emptyIterator();
                            }
                        }

//                        Set<Long> queryCellIds = queryIndexIntervals.keySet();
//                        queryCellIds.removeAll(trackletsCellIds);
//
//                        for (Long queryCellId : queryCellIds) {
//                            List<IndexInterval> indexInterval = queryIndexIntervals.get(queryCellId);
//                            for (int i = 0; i < indexInterval.size(); i++) {
//                                for (int j = indexInterval.get(i).getStart()-1; j <= indexInterval.get(i).getEnd()-1; j++) {
//                                    boolean r = true;
//                                    for (Long trackletsCellId : trackletsCellIds) {
//                                        long[] cube = smallHilbertCurveBr.getValue().point(trackletsCellId);
//                                        double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
//                                        double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));
//                                        double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
//                                        double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));
//                                        if(HilbertUtil.minDistPointToRectangle(trajectoryQuery[j].getLongitude(), trajectoryQuery[j].getLatitude(), xMin, yMin, xMax, yMax)<=epsilon){
//                                            r = false;
//                                            break;
//                                        }
//                                    }
//                                    if(r){return Collections.emptyIterator();}
//                                }
//                            }
//                        }

                        return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, new TrajectorySegment(f._1, ts))).iterator();
                    });


            List<Tuple2<Void,TrajectorySegment>> trajs = results.collect();
            long endTime = System.currentTimeMillis();
            long num = trajs.size();

            long numOfPoints = 0;
            for (Tuple2<Void, TrajectorySegment> voidTrajectoryTuple2 : trajs) {
                numOfPoints = numOfPoints + voidTrajectoryTuple2._2.getSpatialPoints().length;
            }
            int w = (sb.length() == 0 ? 0 : (int) sb.chars().filter(c -> c == ',').count() + 1);

            bw.write((endTime - startTime)+"\t"+trajectoryQuery.length+"\t"+num+"\t"+numOfPoints+"\t"+"true"+"\t"+DataPage.counter+"\t"+w+"\t"+parseAndCubeIndex);
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
        while (br.readLine() != null) {
            queryEndJobs.add(1);
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

            List<Long>[] stages = SparkLogParser.getTimeStages(eventLogFile.getAbsolutePath(), 2);
            List<Long>[] lists = SparkLogParser.getMetricsPerNJobs(eventLogFile.getAbsolutePath(), queryEndJobs);
            try {
                SparkLogParser.enrichQueryAdHocFileWithStagesAndMetricsCondition(fullPathExportedFile, stages, lists);
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
