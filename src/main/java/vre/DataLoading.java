package vre;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.messages.common.Bounds;
import gr.ds.unipi.spatialnodb.messages.common.IndexUtils;
import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import gr.ds.unipi.spatialnodb.messages.common.vre.VRERecord;
import gr.ds.unipi.spatialnodb.messages.common.vre.VRERecordWithKey;
import gr.ds.unipi.spatialnodb.messages.common.vre.VRERecordWithKeyWriteSupport;
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
import vre.impl.SignatureCoder;
import vre.impl.VREKeySerialNumber;
import vre.impl.XZ2Coder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

import static gr.ds.unipi.spatialnodb.AppConfig.loadConfig;

public class DataLoading {
    public static void main(String[] args) throws IOException {

        Config config = loadConfig("data-loading-vre.conf");

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

        final int maxResolution = dataLoading.getInt("maxResolution");
        final boolean wholeWorld = dataLoading.getBoolean("wholeWorld");
        final int m = dataLoading.getInt("m");
        final int n = dataLoading.getInt("n");

        final SmallHilbertCurve hilbertCurve = HilbertCurve.small().bits(bits).dimensions(2);
        final long maxOrdinates = hilbertCurve.maxOrdinate();

        Job job = Job.getInstance();
        job.getConfiguration().setInt("parquet.block.size", 1024*1024*1024);

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setWriteSupportClass(job, VRERecordWithKeyWriteSupport.class);

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

        JavaRDD<Tuple2<double[], VRERecord>> trajectoriesRDD = jsc.textFile(rawDataPath).map(f->f.split(delimiter))
                .filter(fields -> {
                    try {
                        Double.parseDouble(fields[latitudeIndex]);
                        Double.parseDouble(fields[longitudeIndex]);
                        sdf.parse(fields[timeIndex]);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                }).groupBy(f-> f[objectIdIndex], Integer.parseInt(args[0])).map(f->{

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
            return Tuple2.apply(new double[]{minLongitude, minLatitude, maxLongitude, maxLatitude},new VRERecord(objectId, -1, -1, out, minLongitude, minLatitude, maxLongitude, maxLatitude,out[0],out[out.length-1], null));
        }).filter(f-> f._2.getSpatialPoints().length != 1).cache();

        Bounds bounds = trajectoriesRDD.map(t-> t._1).aggregate(
                new Bounds(),
                (acc, ts) -> { acc.add(ts); return acc; },
                (a, b) -> { a.merge(b); return a; }
        );

        final double bminLon = bounds.getMinLongitude();
        final double bminLat = bounds.getMinLatitude();
        final double bmaxLon = bounds.getMaxLongitude()+0.0000001;
        final double bmaxLat = bounds.getMaxLatitude()+0.0000001;


        indexUtils = new IndexUtils(bminLon, bminLat, bmaxLon, bmaxLat, maxOrdinates);

        final double minLon;
        final double minLat;
        final double maxLon;
        final double maxLat;

        if(wholeWorld){
            minLon = -180.0;
            maxLon = 180.0;
            minLat = -90.0;
            maxLat = 90.0;
        }else{
            minLon = bminLon-0.0000001;
            minLat = bminLat-0.0000001;
            maxLon = bmaxLon;
            maxLat = bmaxLat;
        }

        XZ2Coder coder = new XZ2Coder(maxResolution, minLon, maxLon, minLat, maxLat);

        SignatureCoder signatureCoder = new SignatureCoder(m, n);
        JavaPairRDD<Void, VRERecordWithKey> segmentedTrajectoriesRDD = trajectoriesRDD.flatMapToPair(f-> {
            
            String objectId = f._2.getObjectId();
            SpatialPoint[] spts = f._2.getSpatialPoints();
            
            List<Tuple2<Long, VRERecordWithKey>> trajectoryParts = new ArrayList<>();
            List<SpatialPoint> currentPart = new ArrayList<>();
            int serialNumber = 1;

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

                    long code = coder.code(minLongitude, minLatitude, maxLongitude, maxLatitude);
                    SpatialPoint[] currentPartArray = currentPart.toArray(new SpatialPoint[0]);

                    trajectoryParts.add(Tuple2.apply(code, new VRERecordWithKey(code, new VRERecord(objectId, serialNumber++, 1, currentPartArray, minLongitude, minLatitude, maxLongitude, maxLatitude, currentPartArray[0], currentPartArray[currentPartArray.length-1], signatureCoder.signature(currentPartArray, minLongitude, minLatitude, maxLongitude, maxLatitude)))));
                    currentPart.clear();

                    currentPart.add(new SpatialPoint(spts[i].getLongitude(), spts[i].getLatitude()));

                    currentHilValue = hilbertValue;
                }else{
                    currentPart.add(new SpatialPoint(spts[i].getLongitude(), spts[i].getLatitude()));
                }
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

                long code = coder.code(minLongitude, minLatitude, maxLongitude, maxLatitude);
                SpatialPoint[] currentPartArray = currentPart.toArray(new SpatialPoint[0]);

                trajectoryParts.add(Tuple2.apply(code, new VRERecordWithKey(code, new VRERecord(objectId, serialNumber++, 1, currentPartArray, minLongitude, minLatitude, maxLongitude, maxLatitude,currentPartArray[0], currentPartArray[currentPartArray.length-1], signatureCoder.signature(currentPartArray, minLongitude, minLatitude, maxLongitude, maxLatitude)))));
                currentPart.clear();
            }

            if(trajectoryParts.size()==1){
                Tuple2<Long, VRERecordWithKey> trjSeg = trajectoryParts.get(0);
                Tuple2<Long, VRERecordWithKey> newTrjSeg = new Tuple2<>(trjSeg._1, new VRERecordWithKey(trjSeg._1, new VRERecord(trjSeg._2.getRecord().getObjectId(), trjSeg._2.getRecord().getSerialNumber(), 3, trjSeg._2.getRecord().getSpatialPoints(), trjSeg._2.getRecord().getMinLongitude(), trjSeg._2.getRecord().getMinLatitude(), trjSeg._2.getRecord().getMaxLongitude(), trjSeg._2.getRecord().getMaxLatitude(), trjSeg._2.getRecord().getFirstPoint(), trjSeg._2.getRecord().getLastPoint(), trjSeg._2.getRecord().getSignature())));
                trajectoryParts.set(0, newTrjSeg);
            }else{
                Tuple2<Long, VRERecordWithKey> trjSeg = trajectoryParts.get(0);
                Tuple2<Long, VRERecordWithKey> newTrjSeg = new Tuple2<>(trjSeg._1, new VRERecordWithKey(trjSeg._1, new VRERecord(trjSeg._2.getRecord().getObjectId(), trjSeg._2.getRecord().getSerialNumber(), 0, trjSeg._2.getRecord().getSpatialPoints(), trjSeg._2.getRecord().getMinLongitude(), trjSeg._2.getRecord().getMinLatitude(), trjSeg._2.getRecord().getMaxLongitude(), trjSeg._2.getRecord().getMaxLatitude(), trjSeg._2.getRecord().getFirstPoint(), trjSeg._2.getRecord().getLastPoint(), trjSeg._2.getRecord().getSignature())));
                trajectoryParts.set(0, newTrjSeg);

                trjSeg = trajectoryParts.get(trajectoryParts.size()-1);
                newTrjSeg = new Tuple2<>(trjSeg._1, new VRERecordWithKey(trjSeg._1, new VRERecord(trjSeg._2.getRecord().getObjectId(), trjSeg._2.getRecord().getSerialNumber(), 2, trjSeg._2.getRecord().getSpatialPoints(), trjSeg._2.getRecord().getMinLongitude(), trjSeg._2.getRecord().getMinLatitude(), trjSeg._2.getRecord().getMaxLongitude(), trjSeg._2.getRecord().getMaxLatitude(), trjSeg._2.getRecord().getFirstPoint(), trjSeg._2.getRecord().getLastPoint(), trjSeg._2.getRecord().getSignature())));
                trajectoryParts.set(trajectoryParts.size()-1, newTrjSeg);
            }
            return trajectoryParts.iterator();

        }).mapToPair((t)->{return Tuple2.apply(new VREKeySerialNumber(t._1, t._2.getRecord().getSerialNumber()),t._2);}).sortByKey().<Void, VRERecordWithKey>mapToPair(f->Tuple2.apply(null, f._2));

        segmentedTrajectoriesRDD.saveAsNewAPIHadoopFile(writePath+File.separator+"sIndex", Void.class, VRERecordWithKey.class, ParquetOutputFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

        List<Long> keys = segmentedTrajectoriesRDD.map(f->f._2.getKey()).distinct().collect();
        Set<Long> keysSet = new HashSet(keys.size());
        keysSet.addAll(keys);

        Path outputPath = new Path(writePath + "/cache.ser");

        if (writePath.startsWith("hdfs://")) {
            FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
            try (FSDataOutputStream hdfsOut = fs.create(outputPath);
                 ObjectOutputStream out = new ObjectOutputStream(hdfsOut)) {

                out.writeObject(keysSet);
            }
        } else {
            java.nio.file.Path localPath = Paths.get(writePath, "cache.ser");
            try (ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(localPath.toFile()))) {
                out.writeObject(keysSet);
            }
        }

        Tuple2<Long, Long> stats = segmentedTrajectoriesRDD.mapToPair(f->{return Tuple2.apply(f._2.getRecord().getObjectId(), f._2.getKey());}).groupByKey().mapValues(f->{
            HashSet<Long> set = new HashSet<>();
            for (Long s : f) {
                set.add(s);
            }
            return set.size();
        }).values().aggregate(
                new Tuple2<>(0L, 0L),
                (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );

        Tuple2<Integer, Integer> stats1 = segmentedTrajectoriesRDD.mapToPair(f->{return Tuple2.apply(f._2.getRecord().getObjectId(), f._2.getRecord().getSpatialPoints().length);}).values().aggregate(
                new Tuple2<>(0, 0),
                (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),
                (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );

        long uniqueKeys = keys.size();

        long totalParquetFiles = 0;

        if (writePath.startsWith("hdfs://")) {
            Path sIndexPath = new Path(writePath + "/sIndex");
            FileSystem fs = sIndexPath.getFileSystem(job.getConfiguration());
            org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> files = fs.listFiles(sIndexPath, true);
            while (files.hasNext()) {
                if (files.next().getPath().getName().endsWith(".parquet")) {
                    totalParquetFiles++;
                }
            }
        } else {
            java.nio.file.Path root = Paths.get(writePath+ "/sIndex");
            try (Stream<java.nio.file.Path> paths = Files.walk(root)) {
                totalParquetFiles = paths.filter(Files::isRegularFile).filter(path -> path.toString().toLowerCase().endsWith(".parquet")).count();
            }
        }

        Config metadataFile = ConfigFactory.empty()
                .withValue("vre.maxResolution", ConfigValueFactory.fromAnyRef(maxResolution))
                .withValue("vre.m", ConfigValueFactory.fromAnyRef(m))
                .withValue("vre.n", ConfigValueFactory.fromAnyRef(n))
                .withValue("vre.boundaries.minLon", ConfigValueFactory.fromAnyRef(minLon))
                .withValue("vre.boundaries.minLat", ConfigValueFactory.fromAnyRef(minLat))
                .withValue("vre.boundaries.maxLon", ConfigValueFactory.fromAnyRef(maxLon))
                .withValue("vre.boundaries.maxLat", ConfigValueFactory.fromAnyRef(maxLat))
                .withValue("vre.stats.numberOfParquetFiles", ConfigValueFactory.fromAnyRef(totalParquetFiles))
                .withValue("vre.stats.totalNumberOfPoints", ConfigValueFactory.fromAnyRef(stats1._1))
                .withValue("vre.stats.numOfTrajectories", ConfigValueFactory.fromAnyRef(stats._2))
                .withValue("vre.stats.numberOfPointsPerTrajectory", ConfigValueFactory.fromAnyRef((double)stats1._1/stats._2))
                .withValue("vre.stats.averageNumberOfPointsPerTracklet", ConfigValueFactory.fromAnyRef((double) stats1._1/ stats1._2))
                .withValue("vre.stats.uniqueKeys", ConfigValueFactory.fromAnyRef(uniqueKeys))
                .withValue("vre.stats.totalNumberOfTracklets", ConfigValueFactory.fromAnyRef(stats1._2));

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

        try(BufferedWriter bf = new BufferedWriter(new FileWriter(metricsPathExport+File.separator+"data-loading-"+Paths.get(writePath).getFileName().toString()+".txt"))) {
            bf.write("Write Time");
            bf.newLine();
            bf.write(String.valueOf((endTime - startTime)/1000));
        }

        sparkSession.close();
    }

    public static SpatialPoint[] pivots(SpatialPoint[] pts, double xmin, double ymin, double xmax, double ymax) {

        boolean[] pick = new boolean[pts.length];
        pick[0] = true;                 // start point
        pick[pts.length - 1] = true;    // end point

        for (int i = 0; i < pts.length; i++) {
            if (pick[i]) {
                continue;
            }
            double x = pts[i].getLongitude();
            double y = pts[i].getLatitude();
            // "on the border" of the MBR: touches any of the four sides.
            if (eq(x, xmin) || eq(x, xmax) || eq(y, ymin) || eq(y, ymax)) {
                pick[i] = true;
            }
        }

        int count = 0;
        for (boolean b : pick) {
            if (b) {
                count++;
            }
        }
        SpatialPoint[] out = new SpatialPoint[count];
        int j = 0;
        for (int i = 0; i < pts.length; i++) {
            if (pick[i]) {
                out[j++] = pts[i];
            }
        }
        return out;
    }

    private static boolean eq(double a, double b) {
        double diff = Math.abs(a - b);
        if (diff == 0.0) {
            return true;
        }
        double scale = Math.max(1.0, Math.max(Math.abs(a), Math.abs(b)));
        return diff <= 1e-9 * scale;
    }


}
