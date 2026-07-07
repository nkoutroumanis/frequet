package gr.ds.unipi.spatialnodb.queries.frequet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithIntervalMetadata;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithIntervalMetadataReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
import static gr.ds.unipi.spatialnodb.dataloading.HilbertUtil.areTrajectoryPointsDistanceLessThanEpsilonToCube;
import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class Frechet2DQueriesDirectoriesIntervalsDistancesVar2 {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

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
                ).resolve().getConfig("gridHilbert");
            }
        } else {
            metadata = ConfigFactory.parseFile(new File(parquetPath + File.separator + "space.metadata")).resolve().getConfig("gridHilbert");
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
//        ParquetInputFormat.setTaskSideMetaData(job, true);
//        System.out.println(job.getConfiguration().get("parquet.task.side.metadata"));

        ParquetInputFormat.setReadSupportClass(job, TrajectorySegmentWithIntervalMetadataReadSupport.class);

        SparkConf sparkConf = new SparkConf();//.registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class});/*.setMaster("local[1]").set("spark.executor.memory","1g")*/
        sparkConf.setAppName("Similarity Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory", "4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Set<String> directoriesSet = new HashSet<>();
        if (parquetPath.startsWith("hdfs://")) {
            Path stIndexPath = new Path(parquetPath + "/stIndex");
            FileSystem fs = stIndexPath.getFileSystem(job.getConfiguration());
            FileStatus[] statuses = fs.listStatus(stIndexPath);

            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    directoriesSet.add(status.getPath().getName());
                }
            }
        } else {
            File[] directories = new File(parquetPath + File.separator + "stIndex").listFiles(File::isDirectory);
            for (File directory : directories) {
                directoriesSet.add(directory.getName());
            }
        }

//        for (int k = 0; k < 3; k++) {
//            long startTime = System.currentTimeMillis();
//
//            Path dirPath = new Path(parquetPath + "/stIndex");
//            FileSystem fs = dirPath.getFileSystem(job.getConfiguration());
//        System.out.println(">>> Starting Driver-side Deep Metadata Simulation...");
//
//        long totalFilesFound = 0;
//        long totalBlocksEvaluated = 0;
//        long totalReplicaNodesParsed = 0;
//
//        // 2. Deep recursive iteration (This is what newAPIHadoopFile executes)
//
//            System.out.println("Processing directory: " + dirPath.getName());
//
//            RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(dirPath, true);
//
//            while (fileIterator.hasNext()) {
//                LocatedFileStatus fileStatus = fileIterator.next();
//                totalFilesFound++;
//
//                // Fetch the block location array for the file
//                BlockLocation[] blocks = fileStatus.getBlockLocations();
//
//                for (BlockLocation block : blocks) {
//                    totalBlocksEvaluated++;
//
//                    // CRITICAL STEP: Extract the array of physical DataNodes hosting this block
//                    // This mimics Spark's Driver unpacking network packets to find data locality
//                    String[] hosts = block.getHosts();
//
//                    for (String host : hosts) {
//                        totalReplicaNodesParsed++;
//
//                        // Prevent JIT compiler from optimizing away this loop
//                        if (host == null || host.isEmpty()) {
//                            System.out.print("");
//                        }
//                    }
//                }
//            }
//
//
//        long endTime = System.currentTimeMillis();
//        double durationSeconds = (endTime - startTime) / 1000.0;
//
//        // 3. Print the True Overhead Metrics
//            System.out.println("\n=================================================");
//            System.out.println("EXACT DRIVER OVERHEAD METRICS:");
//            System.out.println("=================================================");
//            System.out.println("Total Individual Files:          " + totalFilesFound);
//            System.out.println("Total HDFS Blocks Processed:     " + totalBlocksEvaluated);
//            System.out.println("Total Node Hostnames Evaluated:  " + totalReplicaNodesParsed);
//            System.out.println("Total Driver Execution Time:     " + durationSeconds + " seconds");
//            System.out.println("=================================================");
//
//            String taskSideMeta = job.getConfiguration().get("parquet.task.side.metadata");
//
//            System.out.println("=====================================================");
//            System.out.println("CURRENT VALUE OF parquet.task.side.metadata: " + taskSideMeta);
//            System.out.println("=====================================================");
//
//        }
//        System.exit(7);

//        String targetPath = parquetPath + "/stIndex";
//        job.getConfiguration().set("mapreduce.input.fileinputformat.inputdir", targetPath);
//        job.getConfiguration().set("mapred.input.dir", targetPath);
//
//        job.getConfiguration().set("mapreduce.input.fileinputformat.list-status.num-threads", "36");
//        job.getConfiguration().set("parquet.task.side.metadata", "true");
//        FileInputFormat.setInputPaths(new JobConf(job.getConfiguration()), new Path(targetPath));
//        System.out.println("=== STARTING ARCHITECTURAL BOTTLENECK SIMULATION ===");
//        System.out.println("Target Path: " + targetPath);
//        System.out.println("---------------------------------------------------\n");
//        ParquetInputFormat<Object> inputFormat = new ParquetInputFormat<>();
//        // --- PHASE 1 & 2: NameNode RPC Crawl & Object Generation ---
//        System.out.println("[Executing] Querying NameNode & Generating File Splits...");
//        long phase1Start = System.currentTimeMillis();
//
//        // This is the exact method Spark hangs on before Stage 4
//        List<?> splits = inputFormat.getSplits(job);
//
//        long phase1Time = System.currentTimeMillis() - phase1Start;
//        System.out.println("--> [Success] Generated " + splits.size() + " individual InputSplits.");
//        System.out.println("--> Phase 1 & 2 Duration: " + phase1Time + " ms (" + (phase1Time / 1000.0) + " seconds)\n");
//
//        // --- PHASE 3 & 5: Task Serialization Simulation ---
//        System.out.println("[Executing] Simulating Driver Task Serialization...");
//        long phase2Start = System.currentTimeMillis();
//
//        // Force the JVM to serialize the metadata array, exactly like shipping tasks to executors
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(baos);
//        oos.writeObject(splits);
//        oos.close();
//
//        long phase2Time = System.currentTimeMillis() - phase2Start;
//        double payloadSizeMb = baos.size() / (1024.0 * 1024.0);
//        System.out.println("--> [Success] Serialized Task Payload Size: " + String.format("%.2f", payloadSizeMb) + " MB");
//        System.out.println("--> Phase 3 & 5 Duration: " + phase2Time + " ms (" + (phase2Time / 1000.0) + " seconds)\n");
//
//        // --- TOTAL RESULTS ---
//        long totalDuration = phase1Time + phase2Time;
//        System.out.println("===================================================");
//        System.out.println("TOTAL PRE-STAGE 4 SIMULATION TIME: " + (totalDuration / 1000.0) + " seconds");
//        System.out.println("===================================================");
//
//        System.exit(7);


        String fullPathExportedFile = metricsPath+ File.separator+"frechet-queries-distances-var2-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tNum of Trajectories\tNum of Points\tIssued\tData Pages\tIntersected Cubes\tParse\n");
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

            final double queryMinLongitude = Double.max(minLon,mbrMinLongitude-epsilon);
            final double queryMinLatitude = Double.max(minLat,mbrMinLatitude-epsilon);

            final double queryMaxLongitude = Double.min(maxLon-0.0000001,mbrMaxLongitude+epsilon);
            final double queryMaxLatitude = Double.min(maxLat-0.0000001,mbrMaxLatitude+epsilon);

//            FilterPredicate xAxis = and(gtEq(doubleColumn("minLongitude"), queryMinLongitude), ltEq(doubleColumn("maxLongitude"), queryMaxLongitude));
//            FilterPredicate yAxis = and(gtEq(doubleColumn("minLatitude"), queryMinLatitude), ltEq(doubleColumn("maxLatitude"), queryMaxLatitude));
//
//            ParquetInputFormat.setFilterPredicate(jobIntersected.getConfiguration(), and(xAxis, yAxis));

            long[] hilStart = indexUtils.scale(queryMinLongitude, queryMinLatitude);//HilbertUtil.scaleGeoTemporalPoint(queryMinLongitude, minLon, maxLon,queryMinLatitude, minLat, maxLat, queryMinTimestamp, minTime, maxTime, maxOrdinates);
            long[] hilEnd = indexUtils.scale(queryMaxLongitude, queryMaxLatitude);//HilbertUtil.scaleGeoTemporalPoint(queryMaxLongitude, minLon, maxLon, queryMaxLatitude, minLat, maxLat, queryMaxTimestamp, minTime, maxTime, maxOrdinates);
            Ranges ranges = hilbertCurve.query(hilStart, hilEnd, 0);
            StringBuilder sb = new StringBuilder();

                for (Range range : ranges.toList()) {
                    for (long r = range.low(); r <= range.high(); r++) {
                        if(directoriesSet.contains(String.valueOf(r))) {
                            long[] cube = hilbertCurve.point(r);
                            double xMin = minLon + (cube[0] * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMin = minLat + (cube[1] * (maxLat-minLat)/(maxOrdinates+ 1L));

                            double xMax = minLon + ((cube[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L));
                            double yMax = minLat + ((cube[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L));

                            if(areTrajectoryPointsDistanceLessThanEpsilonToCube(trajectoryQuery, xMin, yMin, xMax, yMax, epsilon)){
                                    sb.append(parquetPath+ File.separator+"stIndex"+File.separator+r+",");
                            }
                        }
                    }
                }

            FilterPredicate xAxis = and(gtEq(doubleColumn("maxLongitude"), trajectoryQuery[0].getLongitude()-epsilon), ltEq(doubleColumn("minLongitude"), trajectoryQuery[0].getLongitude()+epsilon));
            FilterPredicate yAxis = and(gtEq(doubleColumn("maxLatitude"), trajectoryQuery[0].getLatitude()-epsilon), ltEq(doubleColumn("minLatitude"), trajectoryQuery[0].getLatitude()+epsilon));
            FilterPredicate fp = and(xAxis, yAxis);

            for (int i = 1; i < trajectoryQuery.length; i++) {
                xAxis = and(gtEq(doubleColumn("maxLongitude"), trajectoryQuery[i].getLongitude()-epsilon), ltEq(doubleColumn("minLongitude"), trajectoryQuery[i].getLongitude()+epsilon));
                yAxis = and(gtEq(doubleColumn("maxLatitude"), trajectoryQuery[i].getLatitude()-epsilon), ltEq(doubleColumn("minLatitude"), trajectoryQuery[i].getLatitude()+epsilon));
                fp = or(fp, and(xAxis, yAxis));
            }

//            FilterPredicate xAxis = and(gtEq(doubleColumn("minLongitude"), trajectoryQuery[0].getLongitude()-epsilon), ltEq(doubleColumn("maxLongitude"), trajectoryQuery[0].getLongitude()+epsilon));
//            FilterPredicate yAxis = and(gtEq(doubleColumn("minLatitude"), trajectoryQuery[0].getLatitude()-epsilon), ltEq(doubleColumn("maxLatitude"), trajectoryQuery[0].getLatitude()+epsilon));
//            FilterPredicate fp = and(xAxis, yAxis);
//
//            for (int i = 1; i < trajectoryQuery.length; i++) {
//                xAxis = and(gtEq(doubleColumn("minLongitude"), trajectoryQuery[i].getLongitude()-epsilon), ltEq(doubleColumn("maxLongitude"), trajectoryQuery[i].getLongitude()+epsilon));
//                yAxis = and(gtEq(doubleColumn("minLatitude"), trajectoryQuery[i].getLatitude()-epsilon), ltEq(doubleColumn("maxLatitude"), trajectoryQuery[i].getLatitude()+epsilon));
//                fp = or(fp, and(xAxis, yAxis));
//            }
            ParquetInputFormat.setFilterPredicate(job.getConfiguration(), fp);

            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            if(sb.length()==0){
                long endTime = System.currentTimeMillis();
                bw.write((endTime-startTime)+"\t"+0+"\t"+0+"\t"+"false"+"\t"+DataPage.counter+"\t"+0+"\t"+parseAndCubeIndex);
                DataPage.counter = 0;
                bw.newLine();
                continue;
            }

            sb.deleteCharAt(sb.length()-1);
//            job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "134217728");
            JavaPairRDD<Void, TrajectorySegmentWithIntervalMetadata> pairRDDRangeQuery = (JavaPairRDD<Void, TrajectorySegmentWithIntervalMetadata>) jsc.newAPIHadoopFile(sb.toString()/*parquetPath+ File.separator+"stIndex"+File.separator*/, ParquetInputFormat.class, Void.class, TrajectorySegmentWithIntervalMetadata.class, job.getConfiguration());


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

//                        System.out.println(pairRDDRangeQuery.collect().size());
//            System.exit(5);
//            if(pairRDDRangeQuery.collect().isEmpty()){
//                System.exit(5);
//            }


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
            int w = (sb.length() == 0 ? 0 : (int) sb.chars().filter(c -> c == ',').count() + 1);

            bw.write((endTime - startTime)+"\t"+num+"\t"+numOfPoints+"\t"+"true"+"\t"+DataPage.counter+"\t"+w+"\t"+parseAndCubeIndex);
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

}
