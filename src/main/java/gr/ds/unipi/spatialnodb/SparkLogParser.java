package gr.ds.unipi.spatialnodb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class SparkLogParser {

    public static void main(String[] args) throws IOException {
        System.out.println(getMetricsAndTimeStagesPerJob("/Users/nicholaskoutroumanis/Desktop/application_1781548784779_0035"));
    }

    public static void enrichQueryAdHocFileWithMetricsAndTimeStages(String path, List<Long>... stages) throws Exception {
        String line;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
        BufferedReader br = new BufferedReader(new FileReader(path));
        line = br.readLine();
        bw.write(line+"\tStage1\tStage2\tShuffled Remote Bytes\tLocal Bytes Read\tBytes Read\n");
        int i = 0;
        while ((line = br.readLine()) != null) {
            if(line.contains("false")){
                bw.write(line+"\t0\t0\t0\t0\t0\n");
                bw.newLine();
            }else if(line.contains("true")){
                bw.write(line+"\t"+stages[0].get(i)+"\t"+stages[1].get(i)+"\t"+stages[2].get(i)+"\t"+stages[3].get(i)+"\t"+stages[4].get(i));
                bw.newLine();
                i++;
            }else{
                try {
                    throw new Exception("In the line it does not exist true or false.");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if(i!=stages[0].size()){
            throw new RuntimeException("Problem with integrating the info from sparks logs to query file."+i+" -> "+stages[0].size());
        }
        bw.close();
        br.close();
        Files.move(Paths.get(path.replaceAll("\\.[^.]+$", ".tmp")), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void enrichQueryAdHocFileWithMetrics(String path, List<Long>... stages) throws Exception {
        String line;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
        BufferedReader br = new BufferedReader(new FileReader(path));
        line = br.readLine();
        bw.write(line+"\tShuffled Remote Bytes\tLocal Bytes Read\tBytes Read\n");
        int i = 0;
        while ((line = br.readLine()) != null) {
            bw.write(line+"\t"+stages[0].get(i)+"\t"+stages[1].get(i)+"\t"+stages[2].get(i));
            bw.newLine();
            i++;
        }
        if(i!=stages[0].size()){
            throw new RuntimeException("Problem with integrating the info from sparks logs to query file."+i+" -> "+stages[0].size());
        }
        bw.close();
        br.close();
        Files.move(Paths.get(path.replaceAll("\\.[^.]+$", ".tmp")), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
    }

//    public static void enrichMetricsQueryAdHocFile(String path, List<Long>... stages) throws Exception {
//        for (List<Long> list : stages) {
//            for (int i = 0; i < 10; i++) {
//                list.remove(0);
//            }
//        }
//        String line;
//        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
//        BufferedReader br = new BufferedReader(new FileReader(path));
//        line = br.readLine();
//        bw.write(line+"\tStage1 Sum\tStage1 Avg\tStage2 Sum\tStage2 Avg\n");
//
//        line = br.readLine();
//        StringBuilder sb = new StringBuilder();
//        sb.append(stages[0].stream().mapToLong(Long::longValue).sum() + "\t"+ stages[0].stream().mapToLong(Long::longValue).average().getAsDouble());
//        sb.append("\t");
//        sb.append(stages[1].stream().mapToLong(Long::longValue).sum() + "\t"+ stages[1].stream().mapToLong(Long::longValue).average().getAsDouble());
//
//        bw.write(line+"\t"+sb);
//        br.close();
//        bw.close();
//        Files.move(Paths.get(path.replaceAll("\\.[^.]+$", ".tmp")), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
//    }

    public static List<Long>[] getMetricsAndTimeStagesPerJob(String filePath){
        try {
            List<Long> times = new ArrayList<>();
            List<Long> stage1;
            List<Long> stage2;
            List<Long> shuffled = new ArrayList<>();
            List<Long> localBytesReadList = new ArrayList<>();
            List<Long> bytesReadList = new ArrayList<>();

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long submissionTime = 0;
            long completedTime = 0;
            long shuffledBytes = 0;
            long localBytesRead = 0;
            long bytesRead = 0;

            while((line = br.readLine())!=null){
                if(line.contains("\"SparkListenerStageCompleted\"")){
                    int index = line.indexOf("\"Submission Time\":");
                    String line1 = line.substring(index);
                    submissionTime = Long.parseLong(line1.substring(0,line1.indexOf(",")).substring(line1.indexOf(":")+1));

                    index = line.indexOf("\"Completion Time\":");
                    line1 = line.substring(index);
                    completedTime = Long.parseLong(line1.substring(0,line1.indexOf(",")).substring(line1.indexOf(":")+1));

                    times.add((completedTime-submissionTime));
                }

                if(line.contains("\"SparkListenerJobEnd\"")){
                    shuffled.add(shuffledBytes);
                    localBytesReadList.add(localBytesRead);
                    bytesReadList.add(bytesRead);
                    shuffledBytes = 0;
                    localBytesRead = 0;
                    bytesRead = 0;
                }

                if(line.contains("\"Remote Bytes Read\"")){
                    int index = line.indexOf("\"Remote Bytes Read\"");
                    String subline = line.substring(index);
                    shuffledBytes = shuffledBytes + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
                }

                if(line.contains("\"Local Bytes Read\"")){
                    int index = line.indexOf("\"Local Bytes Read\"");
                    String subline = line.substring(index);
                    localBytesRead = localBytesRead + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
                }

                if(line.contains("\"Bytes Read\"")){
                    int index = line.indexOf("\"Bytes Read\"");
                    String subline = line.substring(index);
                    bytesRead = bytesRead + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
                }

            }
            stage1 = new ArrayList<>(times.size()/2);
            stage2 = new ArrayList<>(times.size()/2);

            for (int i = 0; i < times.size(); i++) {
                if(i%2==0){
                    stage1.add(times.get(i));
                }else{
                    stage2.add(times.get(i));
                }
            }
            return new List[]{stage1, stage2, shuffled, localBytesReadList, bytesReadList};
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Long>[] getMetricsPerJob(String filePath, List<Integer> querySparkListenerJobEnd){
        try {
            int queryIndex = 0;
            int counter = 0;
            List<Long> shuffled = new ArrayList<>();
            List<Long> localBytesReadList = new ArrayList<>();
            List<Long> bytesReadList = new ArrayList<>();

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long shuffledBytes = 0;
            long localBytesRead = 0;
            long bytesRead = 0;

            while((line = br.readLine())!=null){
                if(line.contains("\"SparkListenerJobEnd\"")){
                    counter++;
                    if(counter==querySparkListenerJobEnd.get(queryIndex)){
                        shuffled.add(shuffledBytes);
                        localBytesReadList.add(localBytesRead);
                        bytesReadList.add(bytesRead);

                        shuffledBytes = 0;
                        localBytesRead = 0;
                        bytesRead = 0;

                        queryIndex++;
                        counter=0;
                    }
                }

                if(line.contains("\"Remote Bytes Read\"")){
                    int index = line.indexOf("\"Remote Bytes Read\"");
                    String subline = line.substring(index);
                    shuffledBytes = shuffledBytes + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
                }

                if(line.contains("\"Local Bytes Read\"")){
                    int index = line.indexOf("\"Local Bytes Read\"");
                    String subline = line.substring(index);
                    localBytesRead = localBytesRead + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
                }

                if(line.contains("\"Bytes Read\"")){
                    int index = line.indexOf("\"Bytes Read\"");
                    String subline = line.substring(index);
                    bytesRead = bytesRead + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
                }

            }
            return new List[]{shuffled, localBytesReadList, bytesReadList};
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getProperty(String filePath, String property){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long number = 0;
            while((line = br.readLine())!=null){
                if(line.contains(property)){
                    int index = line.indexOf(property);
                    line = line.substring(index);
                    number = number + Long.parseLong(line.substring(0,line.indexOf(",")).substring(line.indexOf(":")+1));
                }
            }
            return String.valueOf(number/(1024*1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getPropertyMax(String filePath, String property){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long number = 0;
            while((line = br.readLine())!=null){
                if(line.contains(property)){
                    int index = line.indexOf(property);
                    line = line.substring(index);
                    long currentNumber = Long.parseLong(line.substring(0,line.indexOf(",")).substring(line.indexOf(":")+1));
                    if(number<currentNumber){
                        number = currentNumber;
                    }
                }
            }
            return String.valueOf(number/(1024*1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    public static String getPropertySumFromMaxesTasks(String filePath, String property){
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(filePath));
//            String line;
//            HashMap<String, Long> hashMap = new HashMap<>();
//            while((line = br.readLine())!=null){
//                if(line.contains(property)){
//
//                    int stageProperty = line.indexOf("Stage ID");
//                    String stageLineProperty = line.substring(stageProperty);
//                    long stageNum = Long.parseLong(stageLineProperty.substring(0,stageLineProperty.indexOf(",")).substring(stageLineProperty.indexOf(":")+1));
//
//                    int taskProperty = line.indexOf("Task ID");
//                    String taskLineProperty = line.substring(taskProperty);
//                    long taskNum = Long.parseLong(taskLineProperty.substring(0,taskLineProperty.indexOf(",")).substring(taskLineProperty.indexOf(":")+1));
//
//                    int indexProperty = line.indexOf(property);
//                    String lineProperty = line.substring(indexProperty);
//                    long currentNumber = Long.parseLong(lineProperty.substring(0,lineProperty.indexOf(",")).substring(lineProperty.indexOf(":")+1));
//
//                    if(hashMap.containsKey(stageNum+":"+taskNum)){
//                        long number = hashMap.get(stageNum+":"+taskNum);
//                        if(number<currentNumber){
//                            hashMap.replace(stageNum+":"+taskNum,currentNumber);
//                        }
//                    }else{
//                        hashMap.put(stageNum+":"+taskNum, currentNumber);
//                    }
//                }
//            }
//
//            long sumValues = 0;
//            for (Long value : hashMap.values()) {
//                sumValues += value;
//            }
//
//            return String.valueOf(sumValues/(1024*1024));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//    public static Tuple3<String, String, String> fileLogAnalyzer(){
//        Path directory = Paths.get(System.getProperty("user.home"));
//        String pattern = "application_";
//        String sparkLogValues ="";
//        String sparkLogMb = "";
//        String sparkLogMaxPeak = "";
//
//        try (Stream<Path> files = Files.list(directory)) {
//            List<Path> paths = files.filter(path -> path.getFileName().toString().startsWith(pattern)).collect(Collectors.toList());
//            if(paths.isEmpty()){
//                throw new Exception("The are no files starting with 'application_' in the path");
//            }else if(paths.size()>1){
//                throw new Exception("The are other files starting with 'application_' in the path");
//            }
//            sparkLogValues = SparkLogParser.getTimeFromStates(paths.get(0).toAbsolutePath().toString());
//            sparkLogMb = SparkLogParser.getProperty(paths.get(0).toAbsolutePath().toString(), "\"Remote Bytes Read\"");
//            sparkLogMaxPeak = SparkLogParser.getPropertyMax(paths.get(0).toAbsolutePath().toString(),"\"Peak Execution Memory\"");
//
//            paths.get(0).toFile().delete();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return Tuple3.apply(sparkLogValues, sparkLogMb, sparkLogMaxPeak);
//    }
//
//    public static Tuple3<String, String, String> fileLogAnalyzer2(){
//        Path directory = Paths.get(System.getProperty("user.home"));
//        String pattern = "application_";
//        String sparkLogValues ="";
//        String sparkLogMb = "";
//        String sparkLogTotalMaxPeak = "";
//
//        try (Stream<Path> files = Files.list(directory)) {
//            List<Path> paths = files.filter(path -> path.getFileName().toString().startsWith(pattern)).collect(Collectors.toList());
//            if(paths.isEmpty()){
//                throw new Exception("The are no files starting with 'application_' in the path");
//            }else if(paths.size()>1){
//                throw new Exception("The are other files starting with 'application_' in the path");
//            }
//            sparkLogValues = SparkLogParser.getTimeFromStates(paths.get(0).toAbsolutePath().toString());
//            sparkLogMb = SparkLogParser.getProperty(paths.get(0).toAbsolutePath().toString(), "\"Remote Bytes Read\"");
//            sparkLogTotalMaxPeak = SparkLogParser.getPropertySumFromMaxesTasks(paths.get(0).toAbsolutePath().toString(),"\"Peak Execution Memory\"");
//
//            paths.get(0).toFile().delete();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return Tuple3.apply(sparkLogValues, sparkLogMb, sparkLogTotalMaxPeak);
//    }

}
