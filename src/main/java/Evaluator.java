import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Evaluator {

    public static class EvaluateResult {
        public float getF_measure() {
            return f_measure;
        }

        public float getAccuracy() {
            return accuracy;
        }

        public EvaluateResult(float f_measure, float accuracy) {
            this.f_measure = f_measure;
            this.accuracy = accuracy;
        }

        private final float f_measure;
        private final float accuracy;
    }

    public static EvaluateResult evaluate(String groundTruthPath, String parsedPath) {
        List<String> groundList = new ArrayList<>();
        List<String> parsedList = new ArrayList<>();
        BufferedReader groundTruthReader = null;
        BufferedReader parsedReader = null;

        try {
            groundTruthReader = new BufferedReader(new FileReader(groundTruthPath));
            parsedReader = new BufferedReader(new FileReader(parsedPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String gl, pl;
        String[] headers = new String[0];
        try {
            headers = groundTruthReader.readLine().split(",");
            parsedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int eventIdIndex = 0;
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("EventId")) eventIdIndex = i;
        }
        try {
            while ((gl = groundTruthReader.readLine()) != null) {
                groundList.add(preprocess(gl).split(",")[eventIdIndex]);
            }
            while ((pl = parsedReader.readLine()) != null) {
                parsedList.add(preprocess(pl).split(",")[eventIdIndex - 1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, Long> groundCounts = groundList.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Map<String, Long> parsedCounts = parsedList.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        long realPairs = 0, parsedPairs = 0;
        for (Long count : groundCounts.values()) {
            if (count > 1) realPairs += count * (count - 1) / 2;
        }
        for (Long count : parsedCounts.values()) {
            if (count > 1) parsedPairs += count * (count - 1) / 2;
        }
        //根据parsedEventId取出的logIds代表这些记录被分为了一类
        int accurateEvents = 0;
        int accuratePairs = 0;
        for (String parsedEventId : parsedCounts.keySet()) {
            List<Integer> logIds = new ArrayList<>();
            for (int i = 0; i < parsedList.size(); i++) {
                if (parsedList.get(i).equals(parsedEventId)) logIds.add(i);
            }
            //这些被解析成为一类的记录，看看它们在groundTruth里面是不是一类。如果是一类，那么这些就是被精确解析的，否则不是。
            //如果它们在groundTruth里面是几类，n*(n-1)/2的结果就会小，精确率和召回率都相应更小。
            List<String> logId_groundList = new ArrayList<>();
            for (int i : logIds) logId_groundList.add(groundList.get(i));
            Map<String, Long> logId_groundCounts = logId_groundList.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            if (logId_groundCounts.size() == 1) {
                int count = 0;
                for (String key : logId_groundCounts.keySet()) {
                    for (String s : groundList) {
                        if (key.equals(s)) count++;
                    }
                }
                if (count == logIds.size()) accurateEvents += count;
            }
            for (long count : logId_groundCounts.values()) {
                if (count > 1) accuratePairs += count * (count - 1) / 2;
            }
        }

        float precision = (float) accuratePairs / parsedPairs;
        float recall = (float) accuratePairs / realPairs;
        float f_measure = 2 * precision * recall / (precision + recall);
        float accuracy = (float) accurateEvents / groundList.size();
        return new EvaluateResult(f_measure, accuracy);
    }

    private static String preprocess(String line) {
        String[] gl1 = line.split("\"");
        for (int i = 0; i < gl1.length; i++) {
            if (i % 2 == 1) {
                gl1[i] = gl1[i].replace(',', ' ');
            }
        }
        StringBuilder glBuilder = new StringBuilder();
        for (String s : gl1) glBuilder.append(s);
        return glBuilder.toString();
    }
}
