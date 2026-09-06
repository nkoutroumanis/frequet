package gr.ds.unipi.spatialnodb.queries.frequet.counts;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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

public class Frechet2DQueriesDirectoriesIntervalsMBRVar2 {
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
        sparkConf.set("spark.eventLog.enabled", "false");
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

        String fullPathExportedFile = metricsPath+ File.separator+"prunings-frechet-queries-mbr-var2-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Query Points\tIntersected Cubes\tAll Prunings Pruned\ttrajectoriesInCells\tpreLoadTrajectories\tpreLoadTrajectories Pruned\tendPointsTrajectories\tendPointsTrajectories Pruned\tlocalPruningTrajectories\tlocalPruningTrajectories Pruned\tgapPruningTrajectories\tgapPruningTrajectories Pruned\tfrechetTrajectories\tfrechetTrajectories Pruned\n");
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

            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), trajectoryQuery[0].getLongitude()-epsilon), ltEq(doubleColumn("minLongitude"), trajectoryQuery[0].getLongitude()+epsilon));
            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), trajectoryQuery[0].getLatitude()-epsilon), ltEq(doubleColumn("minLatitude"), trajectoryQuery[0].getLatitude()+epsilon));
            FilterPredicate fp = and(xAxis, yAxis);

            for (int i = 1; i < trajectoryQuery.length; i++) {
                xAxis = and(gtEq(doubleColumn("maxLongitude"), trajectoryQuery[i].getLongitude()-epsilon), ltEq(doubleColumn("minLongitude"), trajectoryQuery[i].getLongitude()+epsilon));
                yAxis = and(gtEq(doubleColumn("maxLatitude"), trajectoryQuery[i].getLatitude()-epsilon), ltEq(doubleColumn("minLatitude"), trajectoryQuery[i].getLatitude()+epsilon));
                fp = or(fp, and(xAxis, yAxis));
            }

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            if(sb.length()==0){
                long endTime = System.currentTimeMillis();
                bw.write((endTime-startTime)+"\t"+trajectoryQuery.length+"\t"+0+"\t"+0+"\t"+"false"+"\t"+DataPage.counter+"\t"+0+"\t"+parseAndCubeIndex);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }

            sb.deleteCharAt(sb.length()-1);
            job.getConfiguration().unset("parquet.private.read.filter.predicate");
            JavaPairRDD<Long, TrajectorySegmentWithIntervalMetadata> pairRDDRangeQuery = (JavaPairRDD<Long, TrajectorySegmentWithIntervalMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormatWithKey.class, Long.class, TrajectorySegmentWithIntervalMetadata.class, job.getConfiguration());
            long trajectoriesInCells = completeTrajectories(pairRDDRangeQuery).count();

            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), fp);
            pairRDDRangeQuery = (JavaPairRDD<Long, TrajectorySegmentWithIntervalMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormatWithKey.class, Long.class, TrajectorySegmentWithIntervalMetadata.class, job.getConfiguration());
            long prePruningTrajectories = completeTrajectories(pairRDDRangeQuery).count();


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

                return true;
            });
            long endPointsTrajectories = completeTrajectories(pairRDDRangeQuery).count();

            pairRDDRangeQuery = pairRDDRangeQuery.filter(f -> {

                //MBR pruning
                if(HilbertUtil.isMinDistGreaterThan(f._2.getTrajectorySegment().getMinLongitude(), f._2.getTrajectorySegment().getMinLatitude(), f._2.getTrajectorySegment().getMaxLongitude(), f._2.getTrajectorySegment().getMaxLatitude(), trajectoryQuery, epsilon)){
                    return false;
                }

                return true;
            });
            long localPruningTrajectories = completeTrajectories(pairRDDRangeQuery).count();

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

                        return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, new TrajectorySegment(f._1, ts))).iterator();
                    });
            long gapTrajectories = results.count();

            long frechetTrajectories = results.filter(f-> HilbertUtil.frechetDistanceIsLessThanEpsilon(trajectoryQuery, f._2.getSpatialPoints(),epsilon)).count();

            int w = (sb.length() == 0 ? 0 : (int) sb.chars().filter(c -> c == ',').count() + 1);

            bw.write(trajectoryQuery.length+"\t"+w+"\t"+(trajectoriesInCells-gapTrajectories)+"\t"+trajectoriesInCells+"\t"+prePruningTrajectories+"\t"+(trajectoriesInCells-prePruningTrajectories)+"\t"+endPointsTrajectories+"\t"+(prePruningTrajectories-endPointsTrajectories)+"\t"+localPruningTrajectories+"\t"+(endPointsTrajectories-localPruningTrajectories)+"\t"+gapTrajectories+"\t"+(localPruningTrajectories-gapTrajectories)+"\t"+frechetTrajectories+"\t"+(gapTrajectories-frechetTrajectories));
            DataPage.counter = 0;
            bw.newLine();
        }
        bw.close();
        br.close();

        sparkSession.close();

    }

    public static JavaPairRDD<Void, TrajectorySegment> completeTrajectories(JavaPairRDD<Long, TrajectorySegmentWithIntervalMetadata> results){
        return (JavaPairRDD<Void, TrajectorySegment>) results.groupBy(f->f._2().getTrajectorySegment().getObjectId(), 72)
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

                    return Collections.singletonList(new Tuple2<Void, TrajectorySegment>(null, new TrajectorySegment(f._1, ts))).iterator();
                });
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
