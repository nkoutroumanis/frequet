package tman;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import gr.ds.unipi.spatialnodb.dataloading.HilbertUtil;
import gr.ds.unipi.spatialnodb.hadoop.MultipleParquetOutputsFormat;
import gr.ds.unipi.spatialnodb.messages.common.*;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecord;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecordWithKey;
import gr.ds.unipi.spatialnodb.messages.common.tman.TManRecordWithKeyWriteSupport;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegment;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithMetadata;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWithMetadataWriteSupport;
import gr.ds.unipi.spatialnodb.messages.common.trajparquet.TrajectorySegmentWriteSupport;
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
import scala.Tuple4;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrays;
import tman.impl.DPFeatureExtractor;
import tman.impl.OccupancyShapeCoder;
import tman.impl.XZConfig;
import tman.impl.XZTrajectoryIndex;

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

        Config config = loadConfig("data-loading-tman.conf");

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
        final boolean wholeWorld = dataLoading.getBoolean("wholeWorld");
        final int maxResolution = dataLoading.getInt("maxResolution");
        final int alpha = dataLoading.getInt("alpha");
        final int beta = dataLoading.getInt("beta");

        Job job = Job.getInstance();
        job.getConfiguration().setInt("parquet.block.size", 1024*1024*1024);

        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setWriteSupportClass(job, TManRecordWithKeyWriteSupport.class);

        SimpleDateFormat sdf =  new SimpleDateFormat(dateFormat);
        SparkConf sparkConf = new SparkConf().registerKryoClasses(new Class[]{XZTrajectoryIndex.class, XZConfig.class, XZTrajectoryIndex.ShapeCoder.class, XZTrajectoryIndex.ShapeCoder.class});

        sparkConf.setAppName("Trajectory Loading in TMan");
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[*]").set("spark.executor.memory","4g");
        }

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        long startTime = System.currentTimeMillis();

        JavaRDD<Tuple2<double[],TManRecord>> trajectoriesRDD = jsc.textFile(rawDataPath).map(f->f.split(delimiter))
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


            DPFeatureExtractor.DPFeatures d;
            int[] dpPointIndexes=null;
            DPFeatureExtractor.BoundingBox[] dpMbrs=null;

            if(out.length != 1){
                d = DPFeatureExtractor.extract(out);
                dpPointIndexes = d.getPointIndexes();
                dpMbrs = d.getBoundingBoxes();
            }

            return Tuple2.apply(new double[]{minLongitude, minLatitude, maxLongitude, maxLatitude}, new TManRecord(objectId, out, dpPointIndexes, dpMbrs));
        }).filter(f-> f._2.getSpatialPoints().length != 1);

        final double minLon;
        final double minLat;
        final double maxLon;
        final double maxLat;

        if(wholeWorld) {
            minLon =  -180.0;
            minLat = -90.0;
            maxLon = 180.0;
            maxLat = 90.0;
        }else{
            Bounds bounds = trajectoriesRDD.map(t-> t._1).aggregate(
                    new Bounds(),
                    (acc, ts) -> { acc.add(ts); return acc; },
                    (a, b) -> { a.merge(b); return a; }
            );

            minLon = bounds.getMinLongitude()-0.0000001;
            minLat = bounds.getMinLatitude()-0.0000001;
            maxLon = bounds.getMaxLongitude()+0.0000001;
            maxLat = bounds.getMaxLatitude()+0.0000001;
        }


        //determine ID
        XZConfig cfg = new XZConfig(maxResolution,  alpha, beta, minLon, maxLon, minLat, maxLat);
        XZTrajectoryIndex idx = new XZTrajectoryIndex(cfg);

        JavaPairRDD<Long, TManRecordWithKey> trajectoriesWithKey = trajectoriesRDD.mapToPair(f-> {
            long tshape = OccupancyShapeCoder.indexTrajectory(f._2.getSpatialPoints(), idx, f._1, cfg);
            return Tuple2.apply(tshape, new TManRecordWithKey(tshape, f._2));
        }).cache();

        trajectoriesWithKey.sortByKey().mapToPair(f->Tuple2.apply(null, f._2)).saveAsNewAPIHadoopFile(writePath+File.separator+"sIndex", Void.class, TManRecordWithKey.class, ParquetOutputFormat.class, job.getConfiguration());

        long endTime = System.currentTimeMillis();
        System.out.println("Exec Time: "+(endTime-startTime));

        Map<Long, HashSet<Long>> codeShapes = trajectoriesRDD.mapToPair(f->{
            int[]  seq  = idx.homeCellSequence(f._1);
            long   code = idx.quadrantCode(seq);
            long   s    = OccupancyShapeCoder.shapeCodeForTrajectory(f._2.getSpatialPoints(), seq, cfg);
            return Tuple2.apply(code, s);
        }).aggregateByKey(
                        new HashSet<Long>(),
                        (set, s) -> {
                            set.add(s);
                            return set;
                        },
                        (set1, set2) -> {
                            set1.addAll(set2);
                            return set1;
                        }
                ).collectAsMap();

        codeShapes.forEach((k,v)->{System.out.println(k +" "+v);});

        Path outputPath = new Path(writePath + "/cache.ser");

        if (writePath.startsWith("hdfs://")) {
            FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
            try (FSDataOutputStream hdfsOut = fs.create(outputPath);
                 ObjectOutputStream out = new ObjectOutputStream(hdfsOut)) {

                out.writeObject(codeShapes);
            }
        } else {
            java.nio.file.Path localPath = Paths.get(writePath, "cache.ser");
            try (ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(localPath.toFile()))) {
                out.writeObject(codeShapes);
            }
        }

        Tuple2<Integer, Integer> stats = trajectoriesWithKey.aggregate(
                new Tuple2<>(0, 0),
                (acc, t) -> new Tuple2<>(
                        acc._1 + 1,
                        acc._2 + t._2.getRecord().getSpatialPoints().length
                ),
                (a, b) -> new Tuple2<>(
                        a._1 + b._1,
                        a._2 + b._2
                )
        );

        long uniqueKeys = trajectoriesWithKey.keys().distinct().count();

        long totalParquetFiles = 0;

        if (writePath.startsWith("hdfs://")) {
            Path stIndexPath = new Path(writePath);
            FileSystem fs = stIndexPath.getFileSystem(job.getConfiguration());
            org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> files = fs.listFiles(stIndexPath, true);
            while (files.hasNext()) {
                if (files.next().getPath().getName().endsWith(".parquet")) {
                    totalParquetFiles++;
                }
            }
        } else {
            java.nio.file.Path root = Paths.get(writePath);
            try (Stream<java.nio.file.Path> paths = Files.walk(root)) {
                totalParquetFiles = paths.filter(Files::isRegularFile).filter(path -> path.toString().toLowerCase().endsWith(".parquet")).count();
            }
        }

        Config metadataFile = ConfigFactory.empty()
                .withValue("tman.maxResolution", ConfigValueFactory.fromAnyRef(maxResolution))
                .withValue("tman.alpha", ConfigValueFactory.fromAnyRef(alpha))
                .withValue("tman.beta", ConfigValueFactory.fromAnyRef(beta))
                .withValue("tman.boundaries.minLon", ConfigValueFactory.fromAnyRef(minLon))
                .withValue("tman.boundaries.minLat", ConfigValueFactory.fromAnyRef(minLat))
                .withValue("tman.boundaries.maxLon", ConfigValueFactory.fromAnyRef(maxLon))
                .withValue("tman.boundaries.maxLat", ConfigValueFactory.fromAnyRef(maxLat))
                .withValue("tman.stats.numberOfParquetFiles", ConfigValueFactory.fromAnyRef(totalParquetFiles))
                .withValue("tman.stats.totalNumberOfPoints", ConfigValueFactory.fromAnyRef(stats._2))
                .withValue("tman.stats.numOfTrajectories", ConfigValueFactory.fromAnyRef(stats._1))
                .withValue("tman.stats.numberOfPointsPerTrajectory", ConfigValueFactory.fromAnyRef((double)stats._2/stats._1))
                .withValue("tman.stats.uniqueKeys", ConfigValueFactory.fromAnyRef(uniqueKeys));

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

}
