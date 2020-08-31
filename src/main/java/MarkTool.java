import java.io.*;
import java.util.*;

public class MarkTool {
    Map<String, Template> templateMap = new HashMap<>();

    private static class Template {
        public Template(String eventId, String template) {
            this.eventId = eventId;
            this.template = template;
        }

        private String eventId;
        private String template;
    }

    private void markMicloudFe() throws IOException {
        String inputFile = "/home/mi/PycharmProjects/logparse/logs/micloud/micloud_fe_2k.log";
        String outputFile = "/home/mi/PycharmProjects/logparse/logs/micloud/micloud_fe_2k.log_structured.csv";
        String templateFile = "/home/mi/PycharmProjects/logparse/logs/micloud/micloud_fe_2k.log_template.csv";
        BufferedReader reader = null;
        BufferedWriter writer = null;
        BufferedWriter templateWriter = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            writer = new BufferedWriter(new FileWriter(outputFile));
            templateWriter = new BufferedWriter(new FileWriter(templateFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            assert writer != null;
            writer.write("LineId,Level,Date,Time,Thread,Component,Content,EventId,EventTemplate\r\n");
            assert templateWriter != null;
            templateWriter.write("EventId,EventTemplate\r\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String line = null;


        int i = 1;
        while (true) {
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (line == null) break;
            StringBuilder builder = new StringBuilder(i + ",");
            String content = null;

            String[] l1 = line.split(" - ");
            if (l1.length == 2) {
                String[] l2 = l1[0].split("] +\\[");
                content = l1[1];
                content = content.replace("\"", "\"\"");
                String[] l31 = l2[0].replace("[", "").replace("]", "").split(" +");
                String[] l32 = l2[1].replace("[", "").replace("]", "").split(" +");
                String level = l31[0];
                String date = l31[1];
                String time = l31[2];
                String thread = l32[0];
                String component = l32[1];
                builder.append(level).append(",").append(date).append(",\"").append(time).append("\",")
                        .append(thread).append(",").append(component).append(",\"").append(content).append("\",");

                if (content.contains("auth token expired, token")) {
                    builder.append("E1").append(",").append("\"auth token expired, token: <*> token content : <*> now : <*>\"");
                    writeMap("E1", "auth token expired, token: <*> token content : <*> now : <*>");
                } else if (content.contains("check sts failed")) {
                    builder.append("E2").append(",").append("\"check sts failed\"");
                    writeMap("E2", "check sts failed ");
                } else if (content.contains("/xmss/xmss-client/cluster_uploads")) {
                    builder.append("E3").append(",").append("\"/xmss/xmss-client/cluster_uploads is updated to '{\"");
                    writeMap("E3", "/xmss/xmss-client/cluster_uploads is updated to '{");
                } else if (content.contains("remote ip is invalid:unknown")) {
                    builder.append("E4").append(",").append("\"remote ip is invalid:unknown\"");
                    writeMap("E4", "remote ip is invalid:unknown");
                } else if (content.contains("invalid authToken")) {
                    builder.append("E5").append(",").append("\"invalid authToken, nonce error, ts <*>, num <*>, authJSON <*>\"");
                    writeMap("E5", "invalid authToken, nonce error, ts <*>, num <*>, authJSON <*>");
                } else if (content.trim().equals("check auth token nonce error")) {
                    builder.append("E6").append(",").append("\"check auth token nonce error \"");
                    writeMap("E6", "check auth token nonce error ");
                } else if (content.trim().equals("check auth token nonce error [AuthTokenNonceError]")) {
                    builder.append("E7").append(",").append("\"check auth token nonce error [AuthTokenNonceError]\"");
                    writeMap("E7", "check auth token nonce error [AuthTokenNonceError]");
                } else if (content.contains("sts check error")) {
                    builder.append("E8").append(",").append("\"sts check error, errorCode <*> threadId <*>\"");
                    writeMap("E8", "sts check error, errorCode <*> threadId <*>");
                } else if (content.contains("sts check invalid parameter")) {
                    builder.append("E9").append(",").append("\"sts check invalid parameter, path <*>, clientSign: <*>, followupSign: <*>\"");
                    writeMap("E9", "sts check invalid parameter, path <*>, clientSign: <*>, followupSign: <*>");
                } else if (content.contains("check sts sign error")) {
                    builder.append("E10").append(",").append("\"check sts sign error, userId: <*>, reason: <*>\"");
                    writeMap("E10", "check sts sign error, userId: <*>, reason: <*>");
                } else {
                    System.out.printf("line %d is not marked", i);
                }

                try {
                    writer.write(builder.toString() + "\r\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } else {
                System.out.printf("- is not a unique splitter, line: %d%n", i);
                continue;
            }
            i++;
        }
        for (Template template : templateMap.values()) {
            StringBuilder builder1 = new StringBuilder();
            builder1.append(template.eventId).append(",").append(template.template);
            try {
                assert templateWriter != null;
                templateWriter.write(builder1.toString() + "\r\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        templateWriter.flush();
        templateWriter.close();
        writer.flush();
        writer.close();
    }

    private void markMicloudMt() {
        String inputFile = "/home/mi/PycharmProjects/logparse/logs/micloud/micloud_mt_2k.log";
        String outputFile = "/home/mi/PycharmProjects/logparse/logs/micloud/micloud_mt_2k.log_structured.csv";
        String templateFile = "/home/mi/PycharmProjects/logparse/logs/micloud/micloud_mt_2k.log_template.csv";
        BufferedReader reader = null;
        BufferedWriter writer = null;
        BufferedWriter templateWriter = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            writer = new BufferedWriter(new FileWriter(outputFile));
            templateWriter = new BufferedWriter(new FileWriter(templateFile));
            writer.write("Level,Date,Time,Thread,Component,content,EventId,Template,Params\r\n");
            templateWriter.write("EventId,Template\r\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        String line = null;
        int i = 0;

        String reg1 = "FindDeviceMapping record dataCenter not sync, next forward to: (US_OREGON|IN), but redirect is disabled, fid: .*";
        String reg2 = "EK security checking ignores cross datacenter mapping status:.*";
        String reg3 = "SignatureInfo,  fid: .*,content: .* ,sign:.* , checkFindActive :(true|false) ";
        List<String> allRegex = Arrays.asList(reg1, reg2, reg3);
        Map<String, Integer> regMap = new HashMap<>();
        while (true) {
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (line == null) break;
            StringBuilder builder = new StringBuilder(i + ",");
            String content;
            String[] l1 = line.split(" - ");
            if (l1.length == 2) {
                String[] l2 = l1[0].split("] +\\[");
                content = l1[1];
                String[] l31 = l2[0].replace("[", "").replace("]", "").split(" +");
                String[] l32 = l2[1].replace("[", "").replace("]", "").split(" +");
                String level = l31[0];
                String date = l31[1];
                String time = l31[2];
                String thread = l32[0];
                String component = l32[1];
                builder.append(level).append(",").append(date).append(",").append(time).append(",")
                        .append(thread).append(",").append(component).append(",").append(content).append(",");

                for (String reg : allRegex) {
                    if (content.matches(reg)) {
                        regMap.putIfAbsent(reg, 0);
                        regMap.put(reg, regMap.get(reg) + 1);
                    }
                }
            } else {
                System.out.printf("- is not a unique splitter, line: %d%n", i);
                continue;
            }

            i++;
        }

        for (String key : regMap.keySet()) {
            System.out.println(key + ": " + regMap.get(key));
        }
    }

    private void writeMap(String eventId, String template) {
        if (!templateMap.containsKey(eventId))
            templateMap.put(eventId, new Template(eventId, template));
    }

    private List<String> getParams(String original, String template) {
        List<String> result = new ArrayList<>();
        String[] seqOriginal = original.split(" ");
        String[] seqTemplate = template.split(" ");
        for (int i = 0; i < seqOriginal.length; i++) {
            if (!seqOriginal[i].equals(seqTemplate[i])) {
                result.add(seqOriginal[i]);
            }
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        new MarkTool().markMicloudFe();
//        new MarkTool().markMicloudMt();
    }
}
