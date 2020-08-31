import java.io.File;
import java.util.*;

public class DrainBenchmark {
    private static class Setting {
        private final String logFile;
        private final String logFormat;
        private final List<String> regex;
        private final float similarityThreshold;
        private final int depth;

        private Setting(String logFile, String logFormat, List<String> regex, float similarityThreshold, int depth) {
            this.logFile = logFile;
            this.logFormat = logFormat;
            this.regex = regex;
            this.similarityThreshold = similarityThreshold;
            this.depth = depth;
        }
    }

    private static Map<String, Setting> settings = new HashMap<>();

    static {
        settings.put("HDFS", new Setting("HDFS/HDFS_2k.log", "<Date> <Time> <Pid> <Level> <Component>: <Content>", Arrays.asList("blk_-?\\d+", "(\\d+\\.){3}\\d+(:\\d+)?"), 0.5f, 4));
        settings.put("Hadoop", new Setting("Hadoop/Hadoop_2k.log", "<Date> <Time> <Level> \\[<Process>\\] <Component>: <Content>", Collections.singletonList("(\\d+\\.){3}\\d+"), 0.5f, 4));
        settings.put("Spark", new Setting("Spark/Spark_2k.log", "<Date> <Time> <Level> <Component>: <Content>", Arrays.asList("(\\d+\\.){3}\\d+", "\\b[KGTM]?B\\b", "([\\w-]+\\.){2,}[\\w-]+"), 0.5f, 4));
        settings.put("Zookeeper", new Setting("Zookeeper/Zookeeper_2k.log", "<Date> <Time> - <Level>  \\[<Node>:<Component>@<Id>\\] - <Content>", Collections.singletonList("(/|)(\\d+\\.){3}\\d+(:\\d+)?"), 0.5f, 4));
        settings.put("BGL", new Setting("BGL/BGL_2k.log", "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>", Collections.singletonList("core\\.\\d+"), 0.5f, 4));
        settings.put("HPC", new Setting("HPC/HPC_2k.log", "<LogId> <Node> <Component> <State> <Time> <Flag> <Content>", Collections.singletonList("=\\d+"), 0.5f, 4));
        settings.put("Thunderbird", new Setting("Thunderbird/Thunderbird_2k.log", "<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\\[<PID>\\])?: <Content>", Collections.singletonList("(\\d+\\.){3}\\d+"), 0.5f, 4));
        settings.put("Windows", new Setting("Windows/Windows_2k.log", "<Date> <Time>, <Level>                  <Component>    <Content>", Collections.singletonList("0x.*?\\s"), 0.7f, 5));
        settings.put("Linux", new Setting("Linux/Linux_2k.log", "<Month> <Date> <Time> <Level> <Component>(\\[<PID>\\])?: <Content>", Arrays.asList("(\\d+\\.){3}\\d+", "\\d{2}:\\d{2}:\\d{2}"), 0.39f, 6));
        settings.put("Android", new Setting("Android/Android_2k.log", "<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>", Arrays.asList("(/[\\w-]+)+", "([\\w-]+\\.){2,}[\\w-]+", "\\b(\\-?\\+?\\d+)\\b|\\b0[Xx][a-fA-F\\d]+\\b|\\b[a-fA-F\\d]{4,}\\b"), 0.2f, 6));
        settings.put("HealthApp", new Setting("HealthApp/HealthApp_2k.log", "<Time>\\|<Component>\\|<Pid>\\|<Content>", Collections.emptyList(), 0.2f, 4));
        settings.put("Apache", new Setting("Apache/Apache_2k.log", "\\[<Time>\\] \\[<Level>\\] <Content>", Collections.singletonList("(\\d+\\.){3}\\d+"), 0.5f, 4));
        settings.put("Proxifier", new Setting("Proxifier/Proxifier_2k.log", "\\[<Time>\\] <Program> - <Content>", Arrays.asList("<\\d+\\ssec", "([\\w-]+\\.)+[\\w-]+(:\\d+)?", "\\d{2}:\\d{2}(:\\d{2})*", "[KGTM]B"), 0.6f, 3));
        settings.put("OpenSSH", new Setting("OpenSSH/OpenSSH_2k.log", "<Date> <Day> <Time> <Component> sshd\\[<Pid>\\]: <Content>", Arrays.asList("(\\d+\\.){3}\\d+", "([\\w-]+\\.){2,}[\\w-]+"), 0.6f, 5));
        settings.put("OpenStack", new Setting("OpenStack/OpenStack_2k.log", "<Logrecord> <Date> <Time> <Pid> <Level> <Component> \\[<ADDR>\\] <Content>", Arrays.asList("((\\d+\\.){3}\\d+,?)+", "/.+?\\s', r'\\d+"), 0.5f, 5));
        settings.put("Mac", new Setting("Mac/Mac_2k.log", "<Month>  <Date> <Time> <User> <Component>\\[<PID>\\]( \\(<Address>\\))?: <Content>", Collections.singletonList("([\\w-]+\\.){2,}[\\w-]+"), 0.7f, 6));
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String inDir = "src/main/resources/logs/";
        String outDir = "Drain_result/";

        Map<String, Evaluator.EvaluateResult> overAllResult = new HashMap<>();

        for (String key : settings.keySet()) {
            System.out.printf("\n=== Evaluation on %s ===\n", key);
            Setting setting = settings.get(key);
            File file = new File(inDir + setting.logFile);
            String fileName = file.getName();
            new Drain(setting.similarityThreshold, setting.depth, 100, setting.logFormat, setting.regex, inDir + setting.logFile, outDir).parse();

            Evaluator.EvaluateResult result = Evaluator.evaluate(inDir + setting.logFile + "_structured.csv", outDir + fileName + "_structured.csv");

            overAllResult.put(key, result);
        }

        System.out.println("\n=== Overall evaluation results ===\n");
        System.out.println("                    F1_measure          Accuracy");
        for (Map.Entry<String, Evaluator.EvaluateResult> entry : overAllResult.entrySet())
            System.out.printf("%-20s%-20.3f%-20.3f%n", entry.getKey(), entry.getValue().getF_measure(), entry.getValue().getAccuracy());
        System.out.printf("total time cost: %d ms%n", System.currentTimeMillis() - startTime);
    }
}
