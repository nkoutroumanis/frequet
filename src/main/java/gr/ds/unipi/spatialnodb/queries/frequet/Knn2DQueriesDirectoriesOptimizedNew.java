package gr.ds.unipi.spatialnodb.queries.frequet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.spatialnodb.SparkLogParser;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.SpatioTemporalPoint;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.in;

public class Knn2DQueriesDirectoriesOptimizedNew {
    public static void main(String args[]) throws IOException {

        Config config = loadConfig("queries.conf");

        Config dataLoading = config.getConfig("queries");
        final String parquetPath = dataLoading.getString("parquetPath");
        final String queriesFilePath = dataLoading.getString("queriesFilePath");
        final String metricsPath = dataLoading.getString("metricsPath");
        final int k = dataLoading.getInt("k");

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
//        final long minTime = boundaries.getLong("minTime");
        final double maxLon = boundaries.getDouble("maxLon");
        final double maxLat = boundaries.getDouble("maxLat");
//        final long maxTime = boundaries.getLong("maxTime");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        final IndexUtils indexUtils = new IndexUtils(minLon, minLat, maxLon, maxLat, maxOrdinates);;

        Job jobWholeTrajectory = Job.getInstance();
        Job jobTrajectorySegments = Job.getInstance();
        int queriedTrajectoriesCounter = 0;

        ParquetInputFormat.setReadSupportClass(jobWholeTrajectory, TrajectorySegmentReadSupport.class);
        ParquetInputFormat.setReadSupportClass(jobTrajectorySegments, TrajectorySegmentPartialWithMetadataReadSupport.class);
//        ParquetInputFormat.setFilterPredicate(jobTrajectorySegments.getConfiguration(), notEq(longColumn("intervalStart"), null));

        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SpatioTemporalPoint.class,SpatioTemporalPoint[].class, SpatialPoint.class,SpatialPoint[].class, Double.class});
        sparkConf.setAppName("Knn Querying in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Set<String> directoriesSet = new HashSet<>();
        if(parquetPath.startsWith("hdfs://")){
            Path stIndexPath = new Path(parquetPath + "/stIndex");
            FileSystem fs = stIndexPath.getFileSystem(jobWholeTrajectory.getConfiguration());
            FileStatus[] statuses = fs.listStatus(stIndexPath);

            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    directoriesSet.add(status.getPath().getName());
                }
            }
        }else{
            File[] directories = new File(parquetPath+ File.separator+"stIndex").listFiles(File::isDirectory);
            for (File directory : directories) {
                directoriesSet.add(directory.getName());
            }
        }

//        List<Long> times = new ArrayList<>();
//        List<Integer> pages = new ArrayList<>();
        int issuedQueriesTracklets = 0;
        int issuedQueriesFrechetComputation = 0;

        String fullPathExportedFile = metricsPath+ File.separator+"knn-queries-"+Paths.get(parquetPath).getFileName().toString()+"-"+ Paths.get(queriesFilePath).getFileName().toString().replaceFirst("\\.[^.]+$", "")+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(fullPathExportedFile));
        BufferedReader br = new BufferedReader(new FileReader(queriesFilePath));
        bw.write("Time Exec\tQuery Points\tNum of Trajectories\tk-th Distance\tNum of Points\tData Pages\tParse\tMetadata Rounds\tTrajectory Query Rounds\tChecked Trajectories\n");

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

            List<Integer> checked = new ArrayList<>();
            final HashSet<Long> initialCubes = new HashSet<>();

            StringBuilder sb = new StringBuilder();
                for (SpatialPoint spatialPoint : trajectoryQuery) {
                    long[] p = indexUtils.scale(spatialPoint.getLongitude(), spatialPoint.getLatitude());
                    long r = hilbertCurve.index(p);
                    if(!initialCubes.contains(r)){
                        sb.append(parquetPath+File.separator+"stIndex"+File.separator+r+",");
                        initialCubes.add(r);
                    }
                }
            long parseAndCubeIndex = System.currentTimeMillis() - startTime;

            HashMap<String, List<TrajectorySegmentWithMetadata>> identifiedTrajSegments = new HashMap<>();
            HashSet<Binary> flushedTrajectories = new HashSet<>();

            if(sb.length()!=0){
                sb.deleteCharAt(sb.length()-1);
                JavaPairRDD<Void, TrajectorySegmentWithMetadata> pairRDD = (JavaPairRDD<Void, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormat.class, Void.class, TrajectorySegmentWithMetadata.class, jobTrajectorySegments.getConfiguration());
                pairRDD.collect().forEach(seg->{
                    identifiedTrajSegments.compute(seg._2.getTrajectorySegment().getObjectId(), (i, v) -> {
                        if (v == null) {
                            List<TrajectorySegmentWithMetadata> t = new ArrayList<>();
                            t.add(seg._2);
                            return t;
                        } else {
                            v.add(seg._2);
                            v.sort(Comparator.comparingLong(a -> Math.abs(a.getInterval()[0])));
                            return v;
                        }
                    });
                });
                flush(identifiedTrajSegments, flushedTrajectories);
                issuedQueriesTracklets++;
            }

            BoundedPriorityQueue trajectoryQueue = BoundedPriorityQueue.newBoundedPriorityQueue(k);

            HashSet<Long> visitedCubes = new HashSet<>(initialCubes);
            PriorityQueue<CellScore> queueCells = new PriorityQueue<>(Comparator.comparingDouble(CellScore::getScore));

            HashSet<Long> temp = new HashSet<>();
                visitedCubes.forEach(c-> {
                    long[] cube = hilbertCurve.point(c);
                    for (long i = Math.max(0, cube[0]-1); i <= Math.min(maxOrdinates, cube[0]+1); i++) {
                        for (long j = Math.max(0, cube[1]-1); j <= Math.min(maxOrdinates, cube[1]+1); j++) {
                            long cId = hilbertCurve.index(i,j);
                            if(!visitedCubes.contains(cId) && !temp.contains(cId)){
                                long[] cubeNeighbor = hilbertCurve.point(cId);
                                temp.add(cId);
                                queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + (cubeNeighbor[1] * (maxLat-minLat)/(maxOrdinates+ 1L)),minLon + ((cubeNeighbor[0]+1) * (maxLon-minLon)/(maxOrdinates+ 1L)),minLat + ((cubeNeighbor[1]+1) * (maxLat-minLat)/(maxOrdinates+ 1L)),trajectoryQuery)));
                            }
                        }
                    }
                });

            if(flushedTrajectories.size()>=k || queueCells.isEmpty()) {
                ParquetInputFormat.setFilterPredicate(jobWholeTrajectory.getConfiguration(), in(binaryColumn("objectId"), flushedTrajectories) /*filterPredicate*/);
                JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + File.separator + "idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, jobWholeTrajectory.getConfiguration());
                List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatialPoints()))).collect();
                ts.forEach(trajectoryQueue::add);
                checked.add(flushedTrajectories.size());
                queriedTrajectoriesCounter += flushedTrajectories.size();
                flushedTrajectories.clear();
                issuedQueriesFrechetComputation++;
            }

            while((trajectoryQueue.getSize() < k || queueCells.peek().getScore() < trajectoryQueue.getMaxScore()) && !queueCells.isEmpty()) {
                sb.setLength(0);
                int z = visitedCubes.size();
                while(z>0) {
                    long cell = queueCells.poll().getCellId();
                    visitedCubes.add(cell);
                        long[] cube = hilbertCurve.point(cell);
                        for (long i = Math.max(0, cube[0] - 1); i <= Math.min(maxOrdinates, cube[0] + 1); i++) {
                            for (long j = Math.max(0, cube[1] - 1); j <= Math.min(maxOrdinates, cube[1] + 1); j++) {
                                long cId = hilbertCurve.index(i, j);
                                if (!visitedCubes.contains(cId) && !temp.contains(cId)) {
                                    long[] cubeNeighbor = hilbertCurve.point(cId);
                                    temp.add(cId);
                                    queueCells.add(CellScore.newCellScore(cId, HilbertUtil.trajectoryMinDist(minLon + (cubeNeighbor[0] * (maxLon - minLon) / (maxOrdinates + 1L)), minLat + (cubeNeighbor[1] * (maxLat - minLat) / (maxOrdinates + 1L)), minLon + ((cubeNeighbor[0] + 1) * (maxLon - minLon) / (maxOrdinates + 1L)), minLat + ((cubeNeighbor[1] + 1) * (maxLat - minLat) / (maxOrdinates + 1L)), trajectoryQuery)));
                                }
                            }
                        }
                        String j = String.valueOf(hilbertCurve.index(cube[0], cube[1]));
                        if (directoriesSet.contains(j)) {
                            sb.append(parquetPath + File.separator + "stIndex" + File.separator + j + "/,");
                            z--;
                        }
                }

                if(sb.length()!=0){
                    sb.deleteCharAt(sb.length()-1);
                    issuedQueriesTracklets++;
                    JavaPairRDD<Void, TrajectorySegmentWithMetadata> pairRDD = (JavaPairRDD<Void, TrajectorySegmentWithMetadata>) jsc.newAPIHadoopFile(sb.toString(), ParquetInputFormat.class, Void.class, TrajectorySegmentWithMetadata.class, jobTrajectorySegments.getConfiguration());

                    List<TrajectorySegmentWithMetadata> segs = pairRDD.map(f->f._2).collect();

                    for (TrajectorySegmentWithMetadata seg : segs) {
                        identifiedTrajSegments.compute(seg.getTrajectorySegment().getObjectId(), (i, v) -> {
                            if (v == null) {
                                List<TrajectorySegmentWithMetadata> t = new ArrayList<>();
                                t.add(seg);
                                return t;
                            } else {
                                v.add(seg);
                                v.sort(Comparator.comparingLong(a -> Math.abs(a.getInterval()[0])));
                                return v;
                            }
                        });

                        List<TrajectorySegmentWithMetadata> segmentList = identifiedTrajSegments.get(seg.getTrajectorySegment().getObjectId());
                        if(isFull(segmentList) && trajectoryQueue.getSize()==k){
                            if(!canBePruned(segmentList,trajectoryQuery,trajectoryQueue.getMaxScore())){
                                flushedTrajectories.add(Binary.fromString(seg.getTrajectorySegment().getObjectId()));
                            }
                            identifiedTrajSegments.remove(seg.getTrajectorySegment().getObjectId());
                        }
                    }
                }

                if(!flushedTrajectories.isEmpty() && ( flushedTrajectories.size()>= (k-trajectoryQueue.getSize()) || queueCells.isEmpty())){
                    issuedQueriesFrechetComputation++;
                    ParquetInputFormat.setFilterPredicate(jobWholeTrajectory.getConfiguration(), in(binaryColumn("objectId"), flushedTrajectories)/*filterPredicate*/);
                    JavaPairRDD<Void, TrajectorySegment> pairRDD = (JavaPairRDD<Void, TrajectorySegment>) jsc.newAPIHadoopFile(parquetPath + "/" + "idIndex", ParquetInputFormat.class, Void.class, TrajectorySegment.class, jobWholeTrajectory.getConfiguration());

                    List<TrajectoryScore> ts = pairRDD.map(t -> TrajectoryScore.newTrajectoryScore(t._2, HilbertUtil.frechetDistance(trajectoryQuery, t._2.getSpatialPoints()))).collect();
                    ts.forEach(trajectoryQueue::add);
                    queriedTrajectoriesCounter += flushedTrajectories.size();
                    checked.add(flushedTrajectories.size());
                    flushedTrajectories.clear();
                    if(queueCells.isEmpty()){
                        break;
                    }
                }
            }

            System.out.println("The max score is:"+trajectoryQueue.getMaxScore() +"examined trajectoreies are"+queriedTrajectoriesCounter);
            System.out.println(checked);
//            long num =trajectoryQueue.getSize();// trajs.size();

            long endTime = System.currentTimeMillis();
//            times.add((endTime - startTime));
//            pages.add(DataPage.counter);

            int numOfPoints = 0;
            for (TrajectoryScore trajectoryScore : trajectoryQueue.getMaxHeap()) {
                numOfPoints = numOfPoints + trajectoryScore.getTrajectorySegment().getSpatialPoints().length;
            }

            bw.write((endTime - startTime)+"\t"+trajectoryQuery.length+"\t"+trajectoryQueue.getSize()+"\t"+trajectoryQueue.getMaxScore()+"\t"+numOfPoints+"\t"+ DataPage.counter+"\t"+parseAndCubeIndex+"\t"+issuedQueriesTracklets+"\t"+issuedQueriesFrechetComputation+"\t"+queriedTrajectoriesCounter);
            DataPage.counter = 0;
            queriedTrajectoriesCounter = 0;
            issuedQueriesTracklets = 0;
            issuedQueriesFrechetComputation = 0;
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

            List<Long>[] lists = SparkLogParser.getMetricsPerJob(eventLogFile.getAbsolutePath());
            try {
                SparkLogParser.enrichQueryAdHocFileWithMetrics(fullPathExportedFile, lists);
            }catch (Exception e) {
                e.printStackTrace();
            }
            eventLogFile.delete();
        }
    }

    private static void flush(HashMap<String, List<TrajectorySegmentWithMetadata>> identifiedTrajectories, HashSet<Binary> flushedtrajectories){
        identifiedTrajectories.entrySet().removeIf(e->{
            if(e.getValue().size()==1 && e.getValue().get(0).getInterval()[0]==1 && e.getValue().get(0).getInterval()[1]<0){
                flushedtrajectories.add(Binary.fromString(e.getValue().get(0).getTrajectorySegment().getObjectId()));
                return true;
            }else if(e.getValue().get(0).getInterval()[0]==1 && e.getValue().get(e.getValue().size()-1).getInterval()[1]<1){

                long y = e.getValue().get(0).getInterval()[1];
                for (int i = 1; i < e.getValue().size()-1; i++) {
                    if(y+1 != e.getValue().get(i).getInterval()[0]) {return false;}
                    y = e.getValue().get(i).getInterval()[1];
                }
                if(y+1!=e.getValue().get(e.getValue().size()-1).getInterval()[0]){return false;}

                flushedtrajectories.add(Binary.fromString(e.getValue().get(0).getTrajectorySegment().getObjectId()));
                return true;
            }
            return false;
        });
    }

    private static boolean isFull(List<TrajectorySegmentWithMetadata> trajectorySegments){
        if(trajectorySegments.size()==1 && trajectorySegments.get(0).getInterval()[0]==1 && trajectorySegments.get(0).getInterval()[1]<0){
            return true;
        }else if(trajectorySegments.get(0).getInterval()[0]==1 && trajectorySegments.get(trajectorySegments.size()-1).getInterval()[1]<1){
            long y = trajectorySegments.get(0).getInterval()[1];
            for (int i = 1; i < trajectorySegments.size()-1; i++) {
                if(y+1 != trajectorySegments.get(i).getInterval()[0]) {return false;}
                y = trajectorySegments.get(i).getInterval()[1];
            }
            if(y+1!=trajectorySegments.get(trajectorySegments.size()-1).getInterval()[0]){return false;}
            return true;
        }
        return false;
    }

    private static boolean canBePruned(List<TrajectorySegmentWithMetadata> trajectorySegments, SpatialPoint[] trajectoryQuery, double l){
        TrajectorySegmentWithMetadata startSegment = trajectorySegments.get(0);
        TrajectorySegmentWithMetadata endSegment = trajectorySegments.get(trajectorySegments.size()-1);

            //first point of the trajectory
            if(startSegment.getPivots().length==1){
                if (HilbertUtil.euclideanDistance(startSegment.getPivots()[0].getLongitude(), startSegment.getPivots()[0].getLatitude(), trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()) > l) {
                    return true;
                }
            }else{
                if (HilbertUtil.euclideanDistance(startSegment.getPivots()[startSegment.getPivots().length - 2].getLongitude(), startSegment.getPivots()[startSegment.getPivots().length - 2].getLatitude(), trajectoryQuery[0].getLongitude(), trajectoryQuery[0].getLatitude()) > l) {
                    return true;
                }
            }

            //last point of the trajectory
            if(HilbertUtil.euclideanDistance(endSegment.getPivots()[endSegment.getPivots().length-1].getLongitude(),endSegment.getPivots()[endSegment.getPivots().length-1].getLatitude(),trajectoryQuery[trajectoryQuery.length-1].getLongitude(),trajectoryQuery[trajectoryQuery.length-1].getLatitude())>l){
                return true;
            }

        for (TrajectorySegmentWithMetadata trajectorySegment : trajectorySegments) {
            if(trajectorySegment.getInterval()[0]>1 && trajectorySegment.getInterval()[1]>1){
                for (SpatialPoint pivot : trajectorySegment.getPivots()) {
                    if(HilbertUtil.isPointMinDistGreaterThan(pivot.getLongitude(), pivot.getLatitude(), trajectoryQuery, l)){
//                                        if (HilbertUtil.minDistPointToRectangle(pivot.getLongitude(), pivot.getLatitude(), queryMinLongitude, queryMinLatitude, queryMaxLongitude, queryMaxLatitude)> l) {
                        return true;
                    }
                }
            }
        }
        return false;
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

}
