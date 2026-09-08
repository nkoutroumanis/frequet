package gr.ds.unipi.spatialnodb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class SparkLogParser {

//    public static void main(String[] args) throws IOException {
//        System.out.println(getMetricsAndTimeStagesPerJob("/Users/nicholaskoutroumanis/Desktop/application_1781548784779_0035"));
//    }

//    public static void enrichQueryAdHocFileWithMetricsAndTimeStages(String path, List<Long>... stages) throws Exception {
//        String line;
//        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
//        BufferedReader br = new BufferedReader(new FileReader(path));
//        line = br.readLine();
//        bw.write(line+"\tStage1\tStage2\tShuffled Remote Bytes\tLocal Bytes Read\tBytes Read\n");
//        int i = 0;
//        while ((line = br.readLine()) != null) {
//            if(line.contains("false")){
//                bw.write(line+"\t0\t0\t0\t0\t0\n");
//                bw.newLine();
//            }else if(line.contains("true")){
//                bw.write(line+"\t"+stages[0].get(i)+"\t"+stages[1].get(i)+"\t"+stages[2].get(i)+"\t"+stages[3].get(i)+"\t"+stages[4].get(i));
//                bw.newLine();
//                i++;
//            }else{
//                try {
//                    throw new Exception("In the line it does not exist true or false.");
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//        if(i!=stages[0].size()){
//            throw new RuntimeException("Problem with integrating the info from sparks logs to query file."+i+" -> "+stages[0].size());
//        }
//        bw.close();
//        br.close();
//        Files.move(Paths.get(path.replaceAll("\\.[^.]+$", ".tmp")), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
//    }

    public static void enrichQueryAdHocFileWithMetrics(String path, List<Long>[] metrics) throws Exception {
        String line;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
        BufferedReader br = new BufferedReader(new FileReader(path));
        line = br.readLine();
        bw.write(line+"\tShuffled Remote Bytes\tLocal Bytes Read\tBytes Read\n");
        int i = 0;
        while ((line = br.readLine()) != null) {
            bw.write(line+"\t"+metrics[0].get(i)+"\t"+metrics[1].get(i)+"\t"+metrics[2].get(i));
            bw.newLine();
            i++;
        }
        if(i!=metrics[0].size()){
            throw new RuntimeException("Problem with integrating the info from sparks logs to query file."+i+" -> "+metrics[0].size());
        }
        bw.close();
        br.close();
        Files.move(Paths.get(path.replaceAll("\\.[^.]+$", ".tmp")), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void enrichQueryAdHocFileWithStagesAndMetrics(String path, List<Long>[] stages, List<Long>[] metrics) throws Exception {
        String line;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
        BufferedReader br = new BufferedReader(new FileReader(path));
        line = br.readLine();

        int stagesNum = stages.length;

        StringBuilder stagesStringBuilder = new StringBuilder();

        for (int i = 0; i < stagesNum; i++) {
            stagesStringBuilder.append("\t");
            stagesStringBuilder.append("Stage"+(i+1));
        }
        bw.write(line+stagesStringBuilder +"\tShuffled Remote Bytes\tLocal Bytes Read\tBytes Read\n");

        int i = 0;
        while ((line = br.readLine()) != null) {
            stagesStringBuilder.setLength(0);

            for (List<Long> stage : stages) {
                stagesStringBuilder.append("\t");
                stagesStringBuilder.append(stage.get(i));
            }
            bw.write(line+stagesStringBuilder+"\t"+metrics[0].get(i)+"\t"+metrics[1].get(i)+"\t"+metrics[2].get(i));
            bw.newLine();
            i++;
        }

        for (List<Long> stage : stages) {
            if(i!=stage.size()){
                throw new RuntimeException("Problem with integrating the info from sparks logs to query file."+i+" -> "+stages[0].size());
            }
        }

        bw.close();
        br.close();
        Files.move(Paths.get(path.replaceAll("\\.[^.]+$", ".tmp")), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
    }


    public static void enrichQueryAdHocFileWithStagesAndMetricsCondition(String path, List<Long>[] stages, List<Long>[] metrics) throws Exception {
        String line;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.replaceAll("\\.[^.]+$", ".tmp")));
        BufferedReader br = new BufferedReader(new FileReader(path));
        line = br.readLine();

        int stagesNum = stages.length;

        StringBuilder stagesStringBuilder = new StringBuilder();

        for (int i = 0; i < stagesNum; i++) {
            stagesStringBuilder.append("\t");
            stagesStringBuilder.append("Stage"+(i+1));
        }
        bw.write(line+stagesStringBuilder +"\tShuffled Remote Bytes\tLocal Bytes Read\tBytes Read\n");

        int i = 0;
        while ((line = br.readLine()) != null) {
            stagesStringBuilder.setLength(0);
            if(line.contains("false")){
                for (int j = 0; j < stagesNum+metrics.length; j++) {
                    stagesStringBuilder.append("\t");
                    stagesStringBuilder.append("0");
                }
                bw.write(line+stagesStringBuilder);
                bw.newLine();
            }else if(line.contains("true")){
                for (List<Long> stage : stages) {
                    stagesStringBuilder.append("\t");
                    stagesStringBuilder.append(stage.get(i));
                }
                bw.write(line+stagesStringBuilder+"\t"+metrics[0].get(i)+"\t"+metrics[1].get(i)+"\t"+metrics[2].get(i));
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

//    public static List<Long>[] getMetricsAndTimeStagesPerJob(String filePath){
//        try {
//            List<Long> times = new ArrayList<>();
//            List<Long> stage1;
//            List<Long> stage2;
//            List<Long> shuffled = new ArrayList<>();
//            List<Long> localBytesReadList = new ArrayList<>();
//            List<Long> bytesReadList = new ArrayList<>();
//
//            BufferedReader br = new BufferedReader(new FileReader(filePath));
//            String line;
//            long submissionTime = 0;
//            long completedTime = 0;
//            long shuffledBytes = 0;
//            long localBytesRead = 0;
//            long bytesRead = 0;
//
//            while((line = br.readLine())!=null){
//                if(line.contains("\"SparkListenerStageCompleted\"")){
//                    int index = line.indexOf("\"Submission Time\":");
//                    String line1 = line.substring(index);
//                    submissionTime = Long.parseLong(line1.substring(0,line1.indexOf(",")).substring(line1.indexOf(":")+1));
//
//                    index = line.indexOf("\"Completion Time\":");
//                    line1 = line.substring(index);
//                    completedTime = Long.parseLong(line1.substring(0,line1.indexOf(",")).substring(line1.indexOf(":")+1));
//
//                    times.add((completedTime-submissionTime));
//                }
//
//                if(line.contains("\"SparkListenerJobEnd\"")){
//                    shuffled.add(shuffledBytes);
//                    localBytesReadList.add(localBytesRead);
//                    bytesReadList.add(bytesRead);
//                    shuffledBytes = 0;
//                    localBytesRead = 0;
//                    bytesRead = 0;
//                }
//
//                if(line.contains("\"Remote Bytes Read\"")){
//                    int index = line.indexOf("\"Remote Bytes Read\"");
//                    String subline = line.substring(index);
//                    shuffledBytes = shuffledBytes + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
//                }
//
//                if(line.contains("\"Local Bytes Read\"")){
//                    int index = line.indexOf("\"Local Bytes Read\"");
//                    String subline = line.substring(index);
//                    localBytesRead = localBytesRead + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
//                }
//
//                if(line.contains("\"Bytes Read\"")){
//                    int index = line.indexOf("\"Bytes Read\"");
//                    String subline = line.substring(index);
//                    bytesRead = bytesRead + Long.parseLong(subline.substring(0,subline.indexOf(",")).substring(subline.indexOf(":")+1));
//                }
//
//            }
//            stage1 = new ArrayList<>(times.size()/2);
//            stage2 = new ArrayList<>(times.size()/2);
//
//            for (int i = 0; i < times.size(); i++) {
//                if(i%2==0){
//                    stage1.add(times.get(i));
//                }else{
//                    stage2.add(times.get(i));
//                }
//            }
//            return new List[]{stage1, stage2, shuffled, localBytesReadList, bytesReadList};
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }


    public static List<Long>[] getTimeStages(String filePath, int stagesPerGroup){
        try {
            List<Long> times = new ArrayList<>();
            List<Long>[] stages = new List[stagesPerGroup];

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long submissionTime = 0;
            long completedTime = 0;

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
            }

            if (times.size() % stagesPerGroup != 0) {
                throw new IllegalArgumentException(
                        "Number of stages (" + times.size() +
                                ") is not divisible by stagesPerGroup (" + stagesPerGroup + ")"
                );
            }

            for (int i = 0; i < stagesPerGroup; i++) {
                stages[i] = new ArrayList<>(times.size()/stagesPerGroup);
            }

            for (int i = 0; i < times.size(); i++) {
                int stage = i % stagesPerGroup;
                stages[stage].add(times.get(i));
            }
            return stages;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static List<Long>[] getMetricsPerNJobs(String filePath, List<Integer> querySparkListenerJobEnd){
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

}
