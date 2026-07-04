package gr.ds.unipi.spatialnodb.dataloading.trajparquet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.hadoop.MultipleParquetOutputsFormat;
import gr.ds.unipi.spatialnodb.messages.common.*;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import scala.Tuple2;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrays;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class DataLoadingDirectoriesWithWholeTrajectories {
    public static void main(String[] args) throws IOException {

        Config config = loadConfig("data-loading.conf");

        Config dataLoading = config.getConfig("data-loading");
        final String rawDataPath = dataLoading.getString("rawDataPath");
        final String writePath = dataLoading.getString("writePath");
        final int objectIdIndex = dataLoading.getInt("objectIdIndex");
        final int longitudeIndex = dataLoading.getInt("longitudeIndex");
        final int latitudeIndex = dataLoading.getInt("latitudeIndex");
        final int timeIndex = dataLoading.getInt("timeIndex");
        final String dateFormat = dataLoading.getString("dateFormat");
        final String delimiter = dataLoading.getString("delimiter");
        final String metricsPathExport = dataLoading.getString("metricsPathExport");
        final IndexUtils indexUtils;
        Config hilbert = dataLoading.getConfig("hilbert");

        final int bits = hilbert.getInt("bits");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setWriteSupportClass(job, TrajectorySegmentWriteSupport.class);

        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{SmallHilbertCurve.class, HilbertUtil.class});

        sparkConf.setAppName("Trajectory Loading in TrajParquet");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Broadcast smallHilbertCurveBr = jsc.broadcast(hilbertCurve);
        long startTime = System.currentTimeMillis();

        JavaRDD<TrajectorySegment> trajectoriesRDD = jsc.textFile(rawDataPath).map(f->f.split(delimiter)).groupBy(f-> f[objectIdIndex], Integer.parseInt(args[0])).map(f->{

            String objectId = ((Tuple2<String, Iterable<String[]>>) f)._1;
            int counter = 0;

            Iterable<String[]> it0 = ((Tuple2<String, Iterable<String[]>>) f)._2;
            for (String[] strings : it0) {
                counter++;
            }

            double[] x = new double[counter];
            double[] y = new double[counter];
            long[] t = new long[counter];

            int a = 0;
            for (String[] row : f._2) {
                x[a] = Double.parseDouble(row[longitudeIndex]);
                y[a] = Double.parseDouble(row[latitudeIndex]);
                t[a] = sdf.parse(row[timeIndex]).getTime();
                a++;
            }

            int[] idx = new int[counter];
            for (int i = 0; i < counter; i++){ idx[i] = i;}
            IntArrays.quickSort(idx, (a1, b) -> {

                int c = Long.compare(t[a1], t[b]);
                if (c != 0) return c;

                c = Double.compare(x[a1], x[b]);
                if (c != 0) return c;

                return Double.compare(y[a1], y[b]);
            });

            SpatialPoint[] out = new SpatialPoint[counter];

            for (int k = 0; k < counter; k++) {
                int i = idx[k];
                out[k] = new SpatialPoint(x[i], y[i]);
            }

            double minLongitude = Double.MAX_VALUE;
            double minLatitude = Double.MAX_VALUE;

            double maxLongitude = -Double.MAX_VALUE;
            double maxLatitude = -Double.MAX_VALUE;

            for(int j=0; j < counter; j++) {
                if (Double.compare(minLongitude, out[j].getLongitude()) == 1) {
                    minLongitude = out[j].getLongitude();
                }
                if (Double.compare(minLatitude, out[j].getLatitude()) == 1) {
                    minLatitude =out[j].getLatitude();
                }
                if (Double.compare(maxLongitude, out[j].getLongitude()) == -1) {
                    maxLongitude = out[j].getLongitude();
                }
                if (Double.compare(maxLatitude, out[j].getLatitude()) == -1) {
                    maxLatitude = out[j].getLatitude();
                }
            }
            return new TrajectorySegment(objectId, out, minLongitude, minLatitude, maxLongitude, maxLatitude);
        }).cache();

        trajectoriesRDD.mapToPair(f-> Tuple2.apply(f.getObjectId(), f)).sortByKey().mapToPair(f->Tuple2.apply(null, f._2)).saveAsNewAPIHadoopFile(writePath+File.separator+"idIndex", Void.class, TrajectorySegment.class, ParquetOutputFormat.class, job.getConfiguration());

        Bounds bounds = trajectoriesRDD.aggregate(
                        new Bounds(),
                        (acc, ts) -> { acc.add(ts); return acc; },
                        (a, b) -> { a.merge(b); return a; }
                );

        final double minLon = bounds.getMinLongitude();
        final double minLat = bounds.getMinLatitude();
        final double maxLon = bounds.getMaxLongitude()+0.0000001;
        final double maxLat = bounds.getMaxLatitude()+0.0000001;

        indexUtils = new IndexUtils(minLon, minLat, maxLon, maxLat, maxOrdinates);


        ParquetOutputFormat.setWriteSupportClass(job, TrajectorySegmentWithMetadataWriteSupport.class);
        JavaPairRDD segmentedTrajectoriesRDD = trajectoriesRDD.flatMapToPair(f-> {
            
            String objectId = f.getObjectId();
            SpatialPoint[] spts = f.getSpatialPoints();
            
            List<Tuple2<Long, TrajectorySegmentWithMetadata>> trajectoryParts = new ArrayList<>();
            List<SpatialPoint> currentPart = new ArrayList<>();

            //initialize for the currentHilValue
            long intervalStart = 1;
            long intervalEnd = 2;
            long[] hil1 = indexUtils.scale(spts[0]);

            Ranges ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil1, hil1, 0);
            long currentHilValue = ranges.toList().get(0).low();
            currentPart.add(new SpatialPoint(spts[0].getLongitude(), spts[0].getLatitude()));

            for (int i = 1; i < spts.length; i++) {
                long[] hil2 = indexUtils.scale(spts[i]);
                ranges = ((SmallHilbertCurve)smallHilbertCurveBr.getValue()).query(hil2, hil2, 0);
                long hilbertValue = ranges.toList().get(0).low();

                if(currentHilValue != hilbertValue){

                    double minLongitude = Double.MAX_VALUE;
                    double minLatitude = Double.MAX_VALUE;

                    double maxLongitude = -Double.MAX_VALUE;
                    double maxLatitude = -Double.MAX_VALUE;

                    for (int j=0; j <= currentPart.size()-1; j++) {
                        if (Double.compare(minLongitude, currentPart.get(j).getLongitude()) == 1) {
                            minLongitude = currentPart.get(j).getLongitude();
                        }
                        if (Double.compare(minLatitude, currentPart.get(j).getLatitude()) == 1) {
                            minLatitude = currentPart.get(j).getLatitude();
                        }
                        if (Double.compare(maxLongitude, currentPart.get(j).getLongitude()) == -1) {
                            maxLongitude = currentPart.get(j).getLongitude();
                        }
                        if (Double.compare(maxLatitude, currentPart.get(j).getLatitude()) == -1) {
                            maxLatitude = currentPart.get(j).getLatitude();
                        }
                    }

                    List<SpatialPoint> pivots = new ArrayList<>(3);
                    SpatialPoint medoid = findMedoid(currentPart, (minLongitude + maxLongitude) / 2, (minLatitude + maxLatitude) / 2);
                    SpatialPoint fartherFromMedoid = findFartherFrom(currentPart, medoid);
                    SpatialPoint farther = findFartherFrom(currentPart, fartherFromMedoid);

                    pivots.add(medoid);
                    if (!pivots.contains(fartherFromMedoid)) {
                        pivots.add(fartherFromMedoid);
                    }
                    if (!pivots.contains(farther)) {
                        pivots.add(farther);
                    }

                    if(intervalStart==1){
                        SpatialPoint firstPoint = new SpatialPoint(currentPart.get(0).getLongitude(), currentPart.get(0).getLatitude());
                        if(!pivots.contains(firstPoint)){
                            pivots.add(firstPoint);
                        }else{
                            pivots.remove(firstPoint);
                            pivots.add(firstPoint);
                        }
                    }

                    trajectoryParts.add(Tuple2.apply(currentHilValue, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata( new TrajectorySegment(objectId, currentPart.toArray(new SpatialPoint[0]), minLongitude, minLatitude, maxLongitude, maxLatitude), pivots.toArray(new SpatialPoint[0]), new long[]{intervalStart, intervalEnd-1} )));
                    currentPart.clear();

                    currentPart.add(new SpatialPoint(spts[i].getLongitude(), spts[i].getLatitude()));

                    currentHilValue = hilbertValue;
                    intervalStart = intervalEnd;
                }else{
                    currentPart.add(new SpatialPoint(spts[i].getLongitude(), spts[i].getLatitude()));
                }
                intervalEnd++;
            }

            //leftovers in the currentPartList
            if(!currentPart.isEmpty()){
                double minLongitude = Double.MAX_VALUE;
                double minLatitude = Double.MAX_VALUE;

                double maxLongitude = -Double.MAX_VALUE;
                double maxLatitude = -Double.MAX_VALUE;

                for (int j=0; j < currentPart.size(); j++) {
                    if (Double.compare(minLongitude, currentPart.get(j).getLongitude()) == 1) {
                        minLongitude = currentPart.get(j).getLongitude();
                    }
                    if (Double.compare(minLatitude, currentPart.get(j).getLatitude()) == 1) {
                        minLatitude = currentPart.get(j).getLatitude();
                    }
                    if (Double.compare(maxLongitude, currentPart.get(j).getLongitude()) == -1) {
                        maxLongitude = currentPart.get(j).getLongitude();
                    }
                    if (Double.compare(maxLatitude, currentPart.get(j).getLatitude()) == -1) {
                        maxLatitude = currentPart.get(j).getLatitude();
                    }
                }

                List<SpatialPoint> pivots = new ArrayList<>(3);
                SpatialPoint medoid = findMedoid(currentPart, (minLongitude + maxLongitude) / 2, (minLatitude + maxLatitude) / 2);
                SpatialPoint fartherFromMedoid = findFartherFrom(currentPart, medoid);
                SpatialPoint farther = findFartherFrom(currentPart, fartherFromMedoid);

                pivots.add(medoid);
                if (!pivots.contains(fartherFromMedoid)) {
                    pivots.add(fartherFromMedoid);
                }
                if (!pivots.contains(farther)) {
                    pivots.add(farther);
                }
                if(intervalStart==1){
                    SpatialPoint firstPoint = new SpatialPoint(currentPart.get(0).getLongitude(), currentPart.get(0).getLatitude());
                    if(!pivots.contains(firstPoint)){
                        pivots.add(firstPoint);
                    }else{
                        pivots.remove(firstPoint);
                        pivots.add(firstPoint);
                    }
                }

                trajectoryParts.add(Tuple2.apply(currentHilValue, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(objectId, currentPart.toArray(new SpatialPoint[0]), minLongitude, minLatitude, maxLongitude, maxLatitude), pivots.toArray(new SpatialPoint[0]), new long[]{intervalStart, intervalEnd-1})));
                currentPart.clear();
            }

            Tuple2<Long, TrajectorySegmentWithMetadata> trjSeg = trajectoryParts.get(trajectoryParts.size()-1);
            List<SpatialPoint> pivots = new ArrayList<>(Arrays.asList(trjSeg._2().getPivots()));
            SpatialPoint lp = trjSeg._2.getTrajectorySegment().getSpatialPoints()[trjSeg._2.getTrajectorySegment().getSpatialPoints().length-1];
            SpatialPoint lastPoint = new SpatialPoint(lp.getLongitude(), lp.getLatitude());

            if(!pivots.contains(lastPoint)){
                pivots.add(lastPoint);
            }else{
                if(!pivots.get(pivots.size()-1).equals(lastPoint)){
                    pivots.remove(lastPoint);
                    pivots.add(lastPoint);
                }
            }
            Tuple2<Long, TrajectorySegmentWithMetadata> newTrjSeg = new Tuple2<>(trjSeg._1, TrajectorySegmentWithMetadata.newTrajectorySegmentWithMetadata(new TrajectorySegment(trjSeg._2.getTrajectorySegment().getObjectId(), trjSeg._2.getTrajectorySegment().getSpatialPoints(), trjSeg._2.getTrajectorySegment().getMinLongitude(), trjSeg._2.getTrajectorySegment().getMinLatitude(), trjSeg._2.getTrajectorySegment().getMaxLongitude(), trjSeg._2.getTrajectorySegment().getMaxLatitude()), pivots.toArray(new SpatialPoint[0]), new long[]{trjSeg._2.getInterval()[0], trjSeg._2.getInterval()[1]*(-1)}));
            trajectoryParts.set(trajectoryParts.size()-1, newTrjSeg);


//            if(trajectoryParts.size()==7){
//            TrajectorySegment t = trajectoryParts.get(6)._2.getTrajectorySegment();
//            if(t.getSegment()==-7 && t.getSpatioTemporalPoints().length==4){
//                System.out.println(Arrays.toString(t.getSpatioTemporalPoints()));
//                System.out.println("mbr: "+ t.getMinLongitude()+" "+ t.getMinLatitude()+" "+t.getMinTimestamp()+ " - "+t.getMaxLongitude()+" "+ t.getMaxLatitude()+" "+t.getMaxTimestamp());
//                System.exit(1);
//            }}

//            if(trajectoryParts.get(trajectoryParts.size()-1)._2.getTrajectorySegment().getSegment()<-1 && trajectoryParts.get(trajectoryParts.size()-1)._2.getPivots().length>3){
//                System.out.println(Arrays.toString(trajectoryParts.get(trajectoryParts.size()-1)._2.getTrajectorySegment().getSpatioTemporalPoints()));
//                System.out.println("pivots: "+ Arrays.toString(trajectoryParts.get(trajectoryParts.size()-1)._2.getPivots()));
//                System.exit(1);
//            }


//            if(intervalEnd-1!=spts.length){
//                System.exit(1);
//            }
//            for (Tuple2<Long, TrajectorySegmentWithMetadata> trajectoryPart : trajectoryParts) {
//                if(trajectoryPart._2.getTrajectorySegment().getSegment()>1){
//                    if(trajectoryPart._2.getTrajectorySegment().getSpatioTemporalPoints().length>2){
//                        if(!(trajectoryPart._2.getInterval()[1]- trajectoryPart._2.getInterval()[0]+1==trajectoryPart._2.getTrajectorySegment().getSpatioTemporalPoints().length-2)){
//                            System.exit(1);
//                        }
//                    }else{
//                        if(trajectoryPart._2.getInterval()!=null){
//                            System.exit(1);
//                        }
//                    }
//                }
//            }
//            long y = trajectoryParts.get(0)._2.getInterval()[1];
//            for (int i = 1; i < trajectoryParts.size(); i++) {
//                if(trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length>2){
//                    if(y+1 != trajectoryParts.get(i)._2.getInterval()[0])
//                    {
//                        System.out.println(y+" "+trajectoryParts.get(i)._2.getInterval()[0]);
//                        System.exit(1);
//                    }
//                    y=trajectoryParts.get(i)._2.getInterval()[1];
//                }
//            }
//
//            for (int i = 1; i < trajectoryParts.size()-1; i++) {
//                if(trajectoryParts.get(i)._2.getTrajectorySegment().getSpatioTemporalPoints().length==2 && trajectoryParts.get(i)._2.getInterval()!=null){
//                    System.exit(1);
//                }
//            }
//
////            if(trajectoryParts.get(0)._2.getTrajectorySegment().getSegment()==-1){
////                System.out.println(trajectoryParts.get(0)._2.getTrajectorySegment());
////                System.out.println(Arrays.toString(trajectoryParts.get(0)._2.getInterval()));
////                System.exit(1);
////            }
//            for (Tuple2<Long, TrajectorySegmentWithMetadata> trajectoryPart : trajectoryParts) {
//                if(trajectoryPart._2.getPivots()==null && trajectoryPart._2.getInterval()!=null){
//                    System.exit(1);
//                }
//                if(trajectoryPart._2.getPivots()!=null && trajectoryPart._2.getInterval()==null){
//                    System.exit(1);
//                }
//            }

//            if(Double.compare(trajectoryParts.get(0)._2.getTrajectorySegment().getSpatialPoints()[0].getLongitude(),-0.162165)==0 && Double.compare(trajectoryParts.get(0)._2.getTrajectorySegment().getSpatialPoints()[0].getLatitude(),49.576332)==0){
//                for (int i = 0; i < trajectoryParts.size(); i++) {
//                        System.out.println(trajectoryParts.get(i)._1+" - "+ trajectoryParts.get(i)._2.toString());
//                }
//                System.exit(5);
//            }

            return trajectoryParts.iterator();

        }).mapToPair((t)->{return Tuple2.apply(new HilbertKeyLongitude(t._1, t._2.getTrajectorySegment().getMinLongitude()),t._2);}).repartitionAndSortWithinPartitions(new HilbertKeyPartitioner(Integer.parseInt(args[0]))).mapToPair(f->Tuple2.apply(Tuple2.apply(f._1.getHilbertKey()+"/", null), f._2));
        segmentedTrajectoriesRDD.saveAsNewAPIHadoopFile(writePath+File.separator+"stIndex", Void.class, TrajectorySegmentWithMetadata.class, MultipleParquetOutputsFormat.class, job.getConfiguration());

        Tuple2<Long, Long> stats = ((JavaPairRDD<Tuple2<String, Object>,TrajectorySegmentWithMetadata>)segmentedTrajectoriesRDD).mapToPair(f->{return Tuple2.apply(f._2.getTrajectorySegment().getObjectId(), f._1._1);}).groupByKey().mapValues(f->{
            HashSet<String> set = new HashSet<>();
            for (String s : f) {
                set.add(s);
            }
            return set.size();
        }).values().aggregate(
                new Tuple2<>(0L, 0L),
                (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );

        Tuple2<Long, Long> stats1 = trajectoriesRDD.map(f->{
            HashSet<Long> set = new HashSet<>();
            for (SpatialPoint spatialPoint : f.getSpatialPoints()) {
                long[] hil = indexUtils.scale(spatialPoint);
                set.add(((SmallHilbertCurve)smallHilbertCurveBr.getValue()).index(hil));
            }
            return set.size();
        }).aggregate(
                new Tuple2<>(0L, 0L),
                (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );


        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

        Config metadataFile = ConfigFactory.empty()
                .withValue("gridHilbert.bits", ConfigValueFactory.fromAnyRef(bits))
                .withValue("gridHilbert.boundaries.minLon", ConfigValueFactory.fromAnyRef(minLon))
                .withValue("gridHilbert.boundaries.minLat", ConfigValueFactory.fromAnyRef(minLat))
                .withValue("gridHilbert.boundaries.maxLon", ConfigValueFactory.fromAnyRef(maxLon))
                .withValue("gridHilbert.boundaries.maxLat", ConfigValueFactory.fromAnyRef(maxLat))
                .withValue("gridHilbert.averageIntersectedCellsPerTrajectory", ConfigValueFactory.fromAnyRef((double) stats._1 / stats._2))
                .withValue("gridHilbert.averageIntersectedCellsPerPointTrajectory", ConfigValueFactory.fromAnyRef((double) stats1._1 / stats1._2))
                .withValue("gridHilbert.numOfTrajectories", ConfigValueFactory.fromAnyRef(stats._2));

        String json = metadataFile.root().render(
                ConfigRenderOptions.defaults()
                        .setJson(false)
                        .setFormatted(true).setComments(false).setOriginComments(false)

        );

        if(writePath.startsWith("hdfs://")){
            FileSystem fs = FileSystem.get(job.getConfiguration());
            try (FSDataOutputStream out = fs.create(new Path(writePath+"/"+"space.metadata"), true)) {
                out.write(json.getBytes(StandardCharsets.UTF_8));
            }
        }else{
            try (FileWriter fw = new FileWriter(writePath+ File.separator+"space.metadata")) {
                fw.write(json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try(BufferedWriter bf = new BufferedWriter(new FileWriter(metricsPathExport+File.separator+"data-loading-trajparquetDirectoriesWithWholeTrajectories-"+Paths.get(writePath).getFileName().toString()+".txt"))) {
            bf.write("Write Time");
            bf.newLine();
            bf.write(String.valueOf((endTime - startTime)/1000));
        }

        sparkSession.close();
    }

    private static SpatialPoint findMedoid(List<SpatialPoint> spatialPoints, double centroidLon,double centroidLat){
        double minDist = Double.MAX_VALUE;
        SpatialPoint medoid = null;
        for (int i = 0; i < spatialPoints.size(); i++) {
            double distance = HilbertUtil.euclideanDistance(spatialPoints.get(i).getLongitude(),spatialPoints.get(i).getLatitude(),centroidLon,centroidLat);
            if (Double.compare(distance,minDist)==-1) {
                medoid = spatialPoints.get(i);
                minDist = distance;
            }
        }
//        if(medoid==null){
//            System.out.println("is: "+spatioTemporalPoints.size()+" "+part+" "+spatioTemporalPoints.get(0)+" "+spatioTemporalPoints.get(1)+" ");
//        }
        return new SpatialPoint(medoid.getLongitude(), medoid.getLatitude());
    }

    private static SpatialPoint findFartherFrom(List<SpatialPoint> spatialPoints, SpatialPoint spatialPoint){
        double maxDist = -Double.MAX_VALUE;
        SpatialPoint fartherFrom = null;
        for (int i = 0; i < spatialPoints.size(); i++) {
            double distance = HilbertUtil.euclideanDistance(spatialPoints.get(i).getLongitude(),spatialPoints.get(i).getLatitude(),spatialPoint.getLongitude(),spatialPoint.getLatitude());
            if (Double.compare(distance,maxDist)==1) {
                fartherFrom = spatialPoints.get(i);
                maxDist = distance;
            }
        }
        return new SpatialPoint(fartherFrom.getLongitude(), fartherFrom.getLatitude());
    }
}
