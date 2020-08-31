import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * 利用前缀树进行日志模板提取
 *
 **/
public final class Drain {

    private final float similarityThreshold;

    private final int depth;

    private int maxChild;

    /*
     * 模板用<>来识别，<>里面为要识别的内容，<>两边为分隔子串。
     * 分隔子串除了空格，其他都需要记录在正则表达式中，子串可能包含'['、']'、':'这些字符
     * */
    private final String logFormat;

    private List<String> regex;

    private final String inputFile;

    private String outputDir;

    private List<List<String>> data;

    private final List<String> headers;

    private int contentIndex;

    private String logName;

    private static class Node {
        Map<String, Node> map; //非叶子节点有此属性
        List<LogGroup> logGroups; //叶子节点有此属性

        Node() {
            logGroups = new ArrayList<>();
            map = new HashMap<>();
        }
    }

    private static class LogGroup {
        List<Integer> logIds;
        List<String> template;

        LogGroup(int logId, List<String> t) {
            logIds = new ArrayList<>(Collections.singletonList(logId));
            template = t;
        }
    }

    public Drain(float similarityThreshold, int depth, int maxChild, String logFormat, List<String> regex, String inputFile, String outputDir) {
        this.similarityThreshold = similarityThreshold;
        this.depth = depth - 2;
        this.maxChild = maxChild;
        this.logFormat = logFormat;
        this.inputFile = inputFile;
        this.outputDir = outputDir;
        this.regex = regex;
        headers = new ArrayList<>();
        data = new ArrayList<>();
        File file = new File(inputFile);
        logName = file.getName();
    }

    public Drain(String logFormat, List<String> regex, String inputFile, String outputDir) {
        this(0.5f, 4, 100, logFormat, regex, inputFile, outputDir);
    }

    /*
     * 调用Drain的主方法
     * */
    public void parse() {
        System.out.printf("Parsing file: %s%n", inputFile);

        long startTime = System.currentTimeMillis();
        long loadDataStart = System.currentTimeMillis();
        List<List<String>> data = loadData();
        System.out.printf("load data cost: %d ms%n", System.currentTimeMillis() - loadDataStart);
        Map<Integer, Node> nodeMap = new HashMap<>();
        List<LogGroup> allGroups = new ArrayList<>();

        long parseStart = System.currentTimeMillis();
        for (int i = 0; i < data.size(); i++) {
            List<String> logSeq = data.get(i);
            if (logSeq.isEmpty()) continue;
            logSeq.set(contentIndex, "\"" + logSeq.get(contentIndex) + "\"");
            List<String> contentSeq = preProcess(logSeq.get(contentIndex));
            if (!nodeMap.containsKey(contentSeq.size())) {
                nodeMap.put(contentSeq.size(), new Node());
            }
            LogGroup logGroup = treeSearch(nodeMap.get(contentSeq.size()), contentSeq);

            if (logGroup == null) {
                LogGroup newLogGroup = new LogGroup(i, contentSeq);
                allGroups.add(newLogGroup);
                addLogGroupToTree(nodeMap.get(contentSeq.size()), newLogGroup);
            } else {
                List<String> newTemplate = getTemplate(contentSeq, logGroup.template);
                logGroup.logIds.add(i);
                logGroup.template = newTemplate;
            }

            if ((i + 1) % 1000 == 0 || i == data.size() - 1) {
                System.out.printf("Processed %.1f of log lines.%n", (i + 1) * 100.0f / data.size());
            }
        }
        System.out.printf("parse cost: %d ms%n", System.currentTimeMillis() - parseStart);

        long outputStart = System.currentTimeMillis();
        outputResult(data, allGroups);
        System.out.printf("output cost: %d ms%n", System.currentTimeMillis() - outputStart);

        System.out.printf("Parsing done. [Time taken: %d ms]%n", System.currentTimeMillis() - startTime);
    }

    /*
     * 加载日志文件，返回二维表
     * */
    private List<List<String>> loadData() {
        int i = 0;//行数
        List<List<String>> result = new ArrayList<>();
        //把logFormat转换为正则形式
        StringBuilder regexBuilder = new StringBuilder();
        Pattern pattern = Pattern.compile("<[a-zA-Z0-9]+>");
        Matcher matcher = pattern.matcher(logFormat);
        int end = 0;
        while (matcher.find()) {
            regexBuilder.append(logFormat.substring(end, matcher.start()).replaceAll(" +", "\\\\s+"));
            headers.add(matcher.group().substring(1, matcher.group().length() - 1));
            if (headers.get(headers.size() - 1).equals("Content")) contentIndex = headers.size() - 1;
            regexBuilder.append("(?").append(matcher.group()).append(".*?)");
            end = matcher.end();
        }
        regexBuilder.append(logFormat.substring(end).replaceAll(" +", "\\\\s+"));
        Pattern p = Pattern.compile(regexBuilder.toString());
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            String line;
            while ((line = reader.readLine()) != null && line.length() > 0 && i <= 8000000) {
                i++;
                //将line按照logFormat的（正则）形式提取为字符串序列

                Matcher m = p.matcher(line);
                if (m.matches()) { //这一步是最耗时的
                    List<String> logSeq = new ArrayList<>();
                    result.add(logSeq);
                    for (String h : headers) {
                        logSeq.add(m.group(h));
                    }
                }
                //TODO:这里抛出模板格式异常.
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /*
     * 预处理content，将正则匹配到的地方替换成<*>
     * */
    private List<String> preProcess(String content) {
        List<String> result = new ArrayList<>();
        for (String s : content.split(" ")) {
            int len = result.size();
            for (String reg : regex) {
                if (s.matches(reg)) {
                    result.add("<*>");
                    break;
                }
            }
            if (result.size() == len) result.add(s);
        }
        return result;
    }

    private LogGroup treeSearch(Node root, List<String> seq) {
        int currentDepth = 1;

        for (String token : seq) {
            Map<String, Node> map = root.map;
            if (currentDepth >= depth || currentDepth > seq.size()) break;
            currentDepth++;
            if (map.containsKey(token)) root = map.get(token);
            else if (map.containsKey("<*>")) root = map.get("<*>");
            else {
                return null;
            }
        }

        List<LogGroup> logGroups = root.logGroups;

        return fastMatch(logGroups, seq);
    }

    private LogGroup fastMatch(List<LogGroup> logGroups, List<String> seq) {
        float maxSim = 0.0f;
        int maxParamNum = 0;
        LogGroup result = null;
        LogGroup maxSimGroup = null;
        for (LogGroup logGroup : logGroups) {
            List<String> seqTemp = logGroup.template;
            //计算相似度
            float sim = 0.0f;
            int simNum = 0, paramNum = 0;
            assert seq.size() == seqTemp.size();
            for (int i = 0; i < seqTemp.size(); i++) {
                if (seqTemp.get(i).equals("<*>")) {
                    paramNum++;
                    continue;
                }
                if (seqTemp.get(i).equals(seq.get(i))) simNum++;
            }
            sim = (float) simNum / (float) seqTemp.size();
            if (sim > maxSim || (sim == maxSim && paramNum > maxParamNum)) {
                maxSim = sim;
                maxParamNum = paramNum;
                maxSimGroup = logGroup;
            }
        }
        if (maxSim >= similarityThreshold) result = maxSimGroup;
        return result;
    }

    private void addLogGroupToTree(Node root, LogGroup logGroup) {
        int currentDepth = 1;
        for (String token : logGroup.template) {
            Map<String, Node> map = root.map;
            //到达叶子节点
            if (currentDepth >= depth || currentDepth > logGroup.template.size()) {
                root.logGroups.add(logGroup);
                break;
            }
            //往下搜索
            if (map.containsKey(token)) root = map.get(token);
            else {
                //新建节点
                if (hasNumber(token)) {
                    if (!map.containsKey("<*>")) {
                        map.put("<*>", new Node());
                    }
                    root = map.get("<*>");
                } else {
                    if (map.containsKey("<*>")) {
                        if (!token.equals("<*>") && map.size() < maxChild) {
                            map.put(token, new Node());
                            root = map.get(token);
                        } else root = map.get("<*>");
                    } else {
                        if (map.size() + 1 < maxChild) {
                            map.put(token, new Node());
                            root = map.get(token);
                        } else if (map.size() + 1 == maxChild) {
                            map.put("<*>", new Node());
                            root = map.get("<*>");
                        } else root = map.get("<*>");
                    }
                }
            }
            currentDepth++;
        }
    }

    private List<String> getTemplate(List<String> t1, List<String> t2) {
        List<String> res = new ArrayList<>();
        assert t1.size() == t2.size();
        for (int i = 0; i < t2.size(); i++) {
            if (t1.get(i).equals(t2.get(i))) res.add(t1.get(i));
            else res.add("<*>");
        }
        return res;
    }

    private boolean hasNumber(String s) {
        for (char c : s.toCharArray()) {
            if (c >= '0' && c <= '9') return true;
        }
        return false;
    }

    private void outputResult(List<List<String>> data, List<LogGroup> allGroups) {
        File outputPath = new File(outputDir);
        if (!outputPath.exists()) outputPath.mkdirs();
        Set<String> allTemplates = new HashSet<>();

        for (LogGroup logGroup : allGroups) {
            StringBuilder tempBuilder = new StringBuilder();
            for (String s : logGroup.template) tempBuilder.append(s).append(" ");
            try {
                String eventId = DatatypeConverter.printHexBinary(Arrays.copyOfRange(MessageDigest.getInstance("MD5").digest(tempBuilder.toString().trim().getBytes()), 0, 4)).toLowerCase();
                allTemplates.add(eventId + "," + tempBuilder.toString().trim() + "," + logGroup.logIds.size());
                for (Integer id : logGroup.logIds) {
                    data.get(id).add(eventId);
                    data.get(id).add(tempBuilder.toString().trim());
                    String[] contentSeq = data.get(id).get(contentIndex).split(" ");
                    List<String> paramList = new ArrayList<>();
                    for (int i = 0; i < logGroup.template.size(); i++) {
                        if (logGroup.template.get(i).equals("<*>")) {
                            paramList.add(contentSeq[i]);
                        }
                    }
                    data.get(id).add("\"" + paramList.toString() + "\"");
                }
                BufferedWriter stWriter = new BufferedWriter(new FileWriter(outputDir + logName + "_structured.csv"));
                BufferedWriter tmpWriter = new BufferedWriter(new FileWriter(outputDir + logName + "_template.csv"));
                StringBuilder headerBuilder = new StringBuilder();
                for (String header : headers) headerBuilder.append(header).append(",");
                headerBuilder.append("EventId").append(",").append("Template").append(",").append("Params");
                stWriter.write(headerBuilder.toString() + "\r\n");
                for (List<String> seq : data) {
                    if (seq.isEmpty()) continue;
                    StringBuilder dataBuilder = new StringBuilder();
                    for (String s : seq) dataBuilder.append(s).append(",");
                    String line = dataBuilder.toString();
                    stWriter.write(line.substring(0, line.length() - 1) + "\r\n");
                }
                for (String template : allTemplates) {
                    tmpWriter.write(template + "\r\n");
                }
                stWriter.flush();
                tmpWriter.flush();
            } catch (NoSuchAlgorithmException | IOException e) {
                e.printStackTrace();
            }
        }
    }
}
