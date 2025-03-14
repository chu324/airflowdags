package com.tencent.wework;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryOnStatusCodeCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.opencsv.CSVReader;
import com.opencsv.CSVParser;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
import java.io.*;
import java.nio.file.*;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.Instant;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.zip.ZipEntry; 
import java.util.zip.ZipOutputStream; 
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.lang.reflect.Field;

import org.apache.commons.lang3.StringUtils;

public class FetchData {
    private static final Logger logger = Logger.getLogger(FetchData.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static String taskDateStr = null; // 用于存储任务开始时的日期

    // 定义 S3 存储桶名称
    private static final String s3BucketName = "prd".equalsIgnoreCase(System.getenv("env")) 
    ? "175814205108-datafabric-sftp-prd-cn-north-1" 
    : "175826060701-tprdevsftp-sftp-dev-cn-north-1";
    private static final String mediaS3BucketName = "prd".equalsIgnoreCase(System.getenv("env")) 
    ? "175814205108-eds-prd-cn-north-1" 
    : "175826060701-eds-qa-cn-north-1";

    private static final S3Client s3Client = S3Client.builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(Region.CN_NORTH_1)
        .overrideConfiguration(c -> c
            .retryPolicy(RetryPolicy.builder()
                .numRetries(5)
                .backoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
                .retryCondition(RetryOnStatusCodeCondition.create(403, 429, 500, 503))
                .build()
            )
            .apiCallTimeout(Duration.ofMinutes(5))  // 添加超时控制
        )
        .httpClient(UrlConnectionHttpClient.create())
        .build();

    // 定义 raw 文件路径
    private static String rawFilePath = null;

    // 定义 curated 文件路径
    private static String curatedFilePath = null;

    private static int totalTasks;

    private static final String PENDING_TASKS_FILE = "/home/ec2-user/wecom_integration/logs/pending_media_tasks.log";

    // 定时任务
    private static ScheduledExecutorService scheduler;
    
    // 缓存 access_token 和过期时间
    private static String accessToken;
    private static long tokenExpiresTime;

    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILED = "failed";

    // 自定义异常类（作为静态内部类）
    public static class SdkException extends RuntimeException  {
        private final int statusCode;  // 错误码字段
    
        public SdkException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }
    
        // 新增带有Throwable参数的构造函数
        public SdkException(int statusCode, String message, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }
    
        public int getStatusCode() {
            return statusCode;
        }
    }

    /**
     * 获取企业微信的 access_token
     *
     * @param corpid     企业的 ID
     * @param corpsecret 应用的凭证密钥
     * @return access_token 字符串
     */
    private static String getAccessToken(String corpid, String corpsecret) {
    
        if (accessToken != null && tokenExpiresTime > System.currentTimeMillis()) {
            // 如果 token 仍然有效，则直接返回
            return accessToken;
        }
    
        String url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=" + corpid + "&corpsecret=" + corpsecret;
    
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();
    
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    
            if (response.statusCode() == 200) {
                JsonNode rootNode = objectMapper.readTree(response.body());
                if (rootNode.has("errcode") && rootNode.path("errcode").asInt() == 0) {
                    accessToken = rootNode.path("access_token").asText();
                    // 记录 token 的过期时间（当前时间 + expires_in - 60 秒缓冲）
                    tokenExpiresTime = System.currentTimeMillis() + rootNode.path("expires_in").asLong() * 1000 - 60 * 1000;
                    return accessToken;
                } else {
                    logger.severe("获取 access_token 失败，错误码：" + rootNode.path("errcode").asInt() + "，错误信息：" + rootNode.path("errmsg").asText());
                }
            } else {
                logger.severe("HTTP 请求失败，状态码：" + response.statusCode());
            }
        } catch (Exception e) {
            logger.severe("获取 access_token 异常：" + e.getMessage());
        }
    
        return null;
    }

    /**
     * 根据 external_userid 获取 unionid
     *
     * @param externalUserId 外部联系人 ID
     * @return unionid 字符串
     */
    private static String getUnionIdByExternalUserId(String externalUserId) {
        String corpid = "wx1b5619d5190a04e4";
        String corpsecret = "qY6ukRvf83VOi6ZTqVIaKiz93_iDbDGqVLBaSKXJCBs";
    
        String accessToken = getAccessToken(corpid, corpsecret);
        if (accessToken == null) {
            logger.severe("获取 access_token 失败");
            return null;
        }
    
        String apiUrl = "https://qyapi.weixin.qq.com/cgi-bin/externalcontact/get?access_token=" + accessToken + "&external_userid=" + externalUserId;
    
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .build();
    
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    
            if (response.statusCode() == 200) { // Check status code instead of isSuccessful
                JsonNode rootNode = objectMapper.readTree(response.body());
                if (rootNode.has("errcode") && rootNode.path("errcode").asInt() == 0) {
                    return rootNode.path("external_contact").path("unionid").asText();
                } else {
                    logger.severe("API 调用失败，错误码：" + rootNode.path("errcode").asInt() + "，错误信息：" + rootNode.path("errmsg").asText());
                    return null;
                }
            } else {
                logger.severe("HTTP 请求失败，状态码：" + response.statusCode());
                return null;
            }
        } catch (Exception e) {
            logger.severe("获取 unionid 失败：" + e.getMessage());
            return null;
        }
    }

    /**
     * 生成 userid_mapping_yyyymmdd.csv 文件
     *
     * @param chatFilePath      chat_yyyymmdd.csv 文件路径
     * @param mappingFilePath   userid_mapping_yyyymmdd.csv 文件路径
     */
    private static void generateUserIdMappingFile(String chatFilePath, String mappingFilePath) {
        Set<String> externalUserIds = new HashSet<>();
        int totalMappings = 0;
    
        try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(chatFilePath)).build()) {
            csvReader.readNext(); // Skip header
            String[] fields;
            while ((fields = csvReader.readNext()) != null) {
                if (fields.length < 5) continue;
    
                String sender = fields[3];
                String receiver = fields[4];
                if (isExternalUser(sender)) externalUserIds.add(sender);
                if (isExternalUser(receiver)) externalUserIds.add(receiver);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "读取 chat 文件失败", e);
            return;
        }
    
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(mappingFilePath))) {
            csvWriter.writeNext(new String[]{"external_userid", "unionid"});
            for (String externalUserId : externalUserIds) {
                String unionId = getUnionIdByExternalUserId(externalUserId);
                if (unionId != null) {
                    csvWriter.writeNext(new String[]{externalUserId, unionId});
                    totalMappings++;
                }
            }
            // 新增：生成记录数日志
            logger.info("生成 userid_mapping 记录数: " + totalMappings);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "写入 userid_mapping 文件失败", e);
        }
    }

    private static boolean isExternalUser(String userId) {
        return (userId.startsWith("wo") || userId.startsWith("wm")) && !userId.startsWith("wb");
    }

    public static boolean fetchNewData(long sdk) {
        // 设置任务日期
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Shanghai")); // 确保使用北京时间
        taskDateStr = now.format(formatter);
    
        // 初始化 raw 文件路径
        String rawDirPath = "data/raw/" + taskDateStr;
        File rawDir = new File(rawDirPath);
        if (!rawDir.exists()) {
            rawDir.mkdirs(); // 创建目录
        }
        rawFilePath = rawDirPath + "/wecom_chat_" + taskDateStr + ".csv";
    
        // 初始化 curated 文件路径
        String curatedDirPath = "data/curated/" + taskDateStr;
        File curatedDir = new File(curatedDirPath);
        if (!curatedDir.exists()) {
            curatedDir.mkdirs(); // 创建目录
        }
        curatedFilePath = curatedDirPath + "/chat_" + taskDateStr + ".csv";
    
        // 阶段 1: 拉取数据
        if (!fetchData(sdk)) {
            return false;
        }
    
        // 阶段 2: 解密数据并保存到 curated 文件
        boolean decryptSuccess = decryptAndSaveToCurated(sdk);
        if (!decryptSuccess) {
            logger.warning("解密并保存到 curated 文件失败");
        }
    
        // 生成 userid_mapping_yyyymmdd.csv 文件,保留后续使用
        //String mappingFilePath = curatedFilePath.replace("chat_", "userid_mapping_");
        //generateUserIdMappingFile(curatedFilePath, mappingFilePath);
    
        // 阶段 3: 下载媒体文件到 S3
        boolean mediaDownloadSuccess = downloadMediaFilesToS3(sdk);
        if (!mediaDownloadSuccess) {
            logger.severe("媒体文件下载失败率超过阈值");
            //sendSNSErrorMessage("媒体文件下载失败率超过10%");
        }
        // 阶段 4: 压缩并上传 raw 和 curated 文件到 S3
        if (!compressAndUploadFilesToS3()) {
            return false;
        }
    
        return true;
    }
    
    private static boolean fetchData(long sdk) {
        int limit = 100; // 每次拉取的数据量
        int lastSeq = SeqHistoryUtil.loadLastSeq(); // 从 seq history log 中加载上次拉取的 seqid
        boolean hasMoreData = true;
        final long RETRY_INTERVAL = 10 * 1000; // 10秒
        final long MAX_RETRY_TIME = 30 * 60 * 1000; // 30分钟，仅用于 ret=10001 的情况
        long startTime = System.currentTimeMillis(); // 任务开始时间
        int totalFetched = 0; // 记录总拉取量
        long lastLogTime = System.currentTimeMillis(); // 记录上次日志打印时间
    
        boolean isInRetryPeriod = false; // 是否处于波动中
        long retryStartTime = 0; // 当前波动的开始时间
    
        while (hasMoreData) {
            long slice = Finance.NewSlice();
            long batchStartTime = System.currentTimeMillis(); // 记录批次开始时间
            try {
                long ret = Finance.GetChatData(sdk, lastSeq, limit, "", "", 10, slice);
    
                if (ret != 0) {
                    if (ret == 10001 || ret == 10002) {
                        if (!isInRetryPeriod) {
                            isInRetryPeriod = true;
                            retryStartTime = System.currentTimeMillis();
                            logger.info("检测到 ret " + ret + "，开始记录波动时间，retryStartTime: " + retryStartTime);
                        }
                        long elapsedTime = System.currentTimeMillis() - retryStartTime;
                        if (elapsedTime < MAX_RETRY_TIME) {
                            Thread.sleep(RETRY_INTERVAL); // 可能抛出 InterruptedException
                            continue;
                        } else {
                            logger.severe("GetChatData failed, ret: " + ret + ". 当前波动超过最大重试时间.");
                            //sendSNSErrorMessage("GetChatData 持续失败，ret=" + ret + "，当前波动超过最大重试时间。");
                            return false;
                        }
                    } else {
                        logger.severe("GetChatData failed, ret: " + ret + ". 任务中断。");
                        //sendSNSErrorMessage("GetChatData 失败，ret=" + ret + "，任务中断。");
                        return false;
                    }
                }
    
                if (isInRetryPeriod) {
                    isInRetryPeriod = false;
                    retryStartTime = 0;
                    logger.info("成功拉取数据，当前波动结束。");
                }
    
                String content = Finance.GetContentFromSlice(slice);
                if (content == null || content.isEmpty()) {
                    logger.severe("GetContentFromSlice 返回空数据");
                    return false;
                }
    
                appendToRawFile(content);
                JsonNode rootNode = objectMapper.readTree(content);
                JsonNode chatDataNode = rootNode.path("chatdata");
    
                if (chatDataNode.isMissingNode() || !chatDataNode.isArray()) {
                    logger.severe("chatdata 字段缺失或格式错误");
                    return false;
                }
    
                int maxSeqInBatch = lastSeq;
                int batchSize = chatDataNode.size();
                totalFetched += batchSize;
    
                for (JsonNode chatData : chatDataNode) {
                    String seqStr = chatData.path("seq").asText();
                    int currentSeq = Integer.parseInt(seqStr);
                    if (currentSeq > maxSeqInBatch) {
                        maxSeqInBatch = currentSeq;
                    }
                }
    
                lastSeq = maxSeqInBatch + 1;
                SeqHistoryUtil.saveLastSeq(lastSeq);
                long batchDuration = System.currentTimeMillis() - batchStartTime;
    
                // 每 60 秒打印一次日志
                if (System.currentTimeMillis() - lastLogTime >= 60000) {
                    logger.info(String.format(
                        "[任务进度] 已拉取数据: %d 条, 数据范围: seq=%d~%d, 耗时: %dms, 总拉取量: %d 条",
                        batchSize, lastSeq - batchSize, lastSeq - 1, batchDuration, totalFetched
                    ));
                    lastLogTime = System.currentTimeMillis(); // 更新上次日志时间
                }
    
                if (batchSize < limit) {
                    hasMoreData = false;
                    logger.info("所有数据已拉取完成！");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 恢复中断状态
                logger.severe("线程被中断: " + e.getMessage());
                return false;
            } catch (IOException e) {
                logger.severe("解析 JSON 失败: " + e.getMessage());
                return false;
            } catch (Exception e) {
                logger.severe("拉取数据失败: " + e.getMessage());
                if (!isInRetryPeriod) {
                    isInRetryPeriod = true;
                    retryStartTime = System.currentTimeMillis();
                }
                long elapsedTime = System.currentTimeMillis() - retryStartTime;
                if (elapsedTime < MAX_RETRY_TIME) {
                    try {
                        Thread.sleep(RETRY_INTERVAL);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        logger.severe("线程被中断: " + ex.getMessage());
                    }
                    continue;
                } else {
                    logger.severe("当前波动超过最大重试时间.");
                    return false;
                }
            } finally {
                if (slice != 0) {
                    Finance.FreeSlice(slice);
                }
            }
        }
    
        return true;
    }

    private static synchronized void appendToRawFile(String content) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(rawFilePath, true))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            logger.severe("写入raw文件失败: " + e.getMessage());
            throw new UncheckedIOException(e);
        }
    }

    private static String escapeControlCharacters(String content) {
        // 替换控制字符为空格或其他合法字符
        return content.replaceAll("[\\p{Cntrl}]", " ");
    }

    private static boolean decryptAndSaveToCurated(long sdk) {
        int totalCount = 0;
        int successCount = 0;
        int failureCount = 0;
        List<String> failureDetails = new ArrayList<>();
    
        try (BufferedReader rawReader = new BufferedReader(new FileReader(rawFilePath));
             BufferedWriter curatedWriter = new BufferedWriter(new FileWriter(curatedFilePath, true))) {
    
            // 添加表头（不包含 fileext 字段）
            if (new File(curatedFilePath).length() == 0) {
                String header = "seq,msgid,action,sender,receiver,roomid,msgtime,msgtype,sdkfileid,md5sum,raw_json";
                curatedWriter.write(header);
                curatedWriter.newLine();
            }
    
            String line;
            while ((line = rawReader.readLine()) != null) {
                totalCount++;
                JsonNode rootNode = objectMapper.readTree(line);
                JsonNode chatDataNode = rootNode.path("chatdata");
    
                if (chatDataNode.isMissingNode() || !chatDataNode.isArray()) {
                    failureCount++;
                    continue;
                }
    
                for (JsonNode chatData : chatDataNode) {
                    String seqStr = chatData.path("seq").asText();
                    String msgid = chatData.path("msgid").asText();
                    String encryptRandomKey = chatData.path("encrypt_random_key").asText();
                    String encryptChatMsg = chatData.path("encrypt_chat_msg").asText();
    
                    String decryptedMsg = decryptData(sdk, encryptRandomKey, encryptChatMsg);
                    if (decryptedMsg == null) {
                        failureCount++;
                        failureDetails.add("seq=" + seqStr + ", msgid=" + msgid);
                        continue;
                    }
    
                    successCount++;
                    String escapedDecryptedMsg = escapeControlCharacters(decryptedMsg);
                    JsonNode decryptedRootNode;
                    try {
                        decryptedRootNode = objectMapper.readTree(escapedDecryptedMsg);
                    } catch (Exception e) {
                        logger.warning("解析解密后的JSON失败: " + e.getMessage());
                        failureCount++;
                        continue;
                    }
    
                    String action = decryptedRootNode.path("action").asText();
                    String sender = decryptedRootNode.path("from").asText();
                    JsonNode tolistNode = decryptedRootNode.path("tolist");
                    String roomid = decryptedRootNode.path("roomid").asText();
                    long msgtime = decryptedRootNode.path("msgtime").asLong();
                    String msgtype = decryptedRootNode.path("msgtype").asText();
                    String sdkfileid = extractSeqFileId(decryptedRootNode, msgtype);
                    String md5sum = extractMd5Sum(decryptedRootNode, msgtype);
                    String fileext = extractFileExt(decryptedRootNode, msgtype); // 提取 fileext
    
                    JsonNode rawJsonNode = decryptedRootNode.path(msgtype);
                    String rawJson;
                    try {
                        // 验证JSON合法性
                        objectMapper.readTree(rawJsonNode.toString());
                        rawJson = rawJsonNode.toString();
                    } catch (Exception e) {
                        logger.warning("无效的JSON内容，替换为默认值");
                        rawJson = "{}"; // 替换空JSON
                    }
    
                    Instant instant = Instant.ofEpochMilli(msgtime);
                    ZonedDateTime beijingTime = instant.atZone(ZoneId.of("Asia/Shanghai"));
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String beijingTimeStr = beijingTime.format(formatter);
    
                    String escapedRawJson = escapeCsvField(rawJson);
    
                    // 写入 curated 文件（不包含 fileext 字段）
                    if (tolistNode.isArray()) {
                        for (JsonNode to : tolistNode) {
                            String receiver = to.asText();
                            curatedWriter.write(String.join(",",
                                seqStr, msgid, action, sender, receiver, roomid,
                                beijingTimeStr, msgtype, sdkfileid, md5sum, escapedRawJson
                            ));
                            curatedWriter.newLine();
                        }
                    } else {
                        String receiver = tolistNode.toString();
                        curatedWriter.write(String.join(",",
                            seqStr, msgid, action, sender, receiver, roomid,
                            beijingTimeStr, msgtype, sdkfileid, md5sum, escapedRawJson
                        ));
                        curatedWriter.newLine();
                    }
                }
            }
    
            logger.info(String.format("解密完成: 总数据条数=%d, 成功=%d, 失败=%d", totalCount, successCount, failureCount));
            if (!failureDetails.isEmpty()) {
                logger.info("解密失败的数据: " + String.join("; ", failureDetails));
            }
            return true;
        } catch (Exception e) {
            logger.severe("解密并保存到 curated 文件失败: " + e.getMessage());
            return false;
        }
    }
    
    // 新增方法：提取 md5sum
    private static String extractMd5Sum(JsonNode decryptedRootNode, String msgtype) {
        JsonNode msgTypeNode = decryptedRootNode.path(msgtype);
        return msgTypeNode.has("md5sum") ? msgTypeNode.path("md5sum").asText() : "";
    }

    private static String extractFileExt(JsonNode decryptedRootNode, String msgtype) {
        if (!"file".equals(msgtype)) {
            return ""; // 只有文件类型才有 fileext
        }
    
        JsonNode fileNode = decryptedRootNode.path("file");
        if (fileNode.isMissingNode()) {
            return "";
        }
    
        return fileNode.path("fileext").asText("");
    }

    /**
     * 生成媒体文件清单JSON，返回有效任务数
     */
    private static int generateMediaFilesJSON(String chatFilePath, String mediaFilesPath) {
        int validCount = 0;
        int skippedCount = 0;
        int duplicateCount = 0;
        int totalCount = 0;
        Set<String> uniqueTaskKeys = new HashSet<>();
    
        CSVParser parser = new CSVParserBuilder()
            .withSeparator(',')
            .withQuoteChar('"')
            .withEscapeChar('\\')
            .withIgnoreQuotations(true)
            .withStrictQuotes(false)
            .build();
    
        try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(chatFilePath));
             CSVReader csvReader = new CSVReaderBuilder(lineNumberReader)
                 .withCSVParser(parser)
                 .withSkipLines(1)
                 .build();
             JsonGenerator jsonGenerator = objectMapper.getFactory()
                 .createGenerator(Files.newBufferedWriter(Paths.get(mediaFilesPath)))
                 .useDefaultPrettyPrinter()) {
    
            jsonGenerator.writeStartArray();
    
            String[] record;
            while (true) {
                try {
                    record = csvReader.readNext();
                    if (record == null) break;
                } catch (CsvValidationException e) {
                    int currentLine = lineNumberReader.getLineNumber();
                    logger.warning("跳过格式错误的行: " + currentLine);
                    skippedCount++;
                    continue;
                }
    
                totalCount++;
                int currentLine = lineNumberReader.getLineNumber();
    
                try {
                    if (record.length < 11) { // 确保有足够的字段
                        logger.warning(String.format("行 %d 字段不足（%d/11），跳过处理", currentLine, record.length));
                        skippedCount++;
                        continue;
                    }
    
                    String msgtype = record[7].trim();
                    String sdkfileid = record[8].trim();
                    String md5sum = record[9].trim();
                    String fileext = record[10].trim(); // 从 CSV 文件中读取 fileext
    
                    if (StringUtils.isAnyBlank(msgtype, sdkfileid, md5sum, fileext)) {
                        skippedCount++;
                        continue;
                    }
    
                    if (!isValidMsgTypeForMedia(msgtype)) {
                        logger.warning(String.format("行 %d 无效消息类型: %s", currentLine, msgtype));
                        skippedCount++;
                        continue;
                    }
    
                    if (!sdkfileid.matches("^[A-Za-z0-9+/=]{32,}$")) {
                        logger.warning(String.format("行 %d 无效文件ID格式: %s", currentLine, sdkfileid));
                        skippedCount++;
                        continue;
                    }
    
                    if (!isValidMD5(md5sum)) {
                        logger.warning(String.format("行 %d 无效MD5格式: %s", currentLine, md5sum));
                        skippedCount++;
                        continue;
                    }
    
                    String uniqueKey = msgtype + "|" + md5sum;
                    if (uniqueTaskKeys.contains(uniqueKey)) {
                        duplicateCount++;
                        logger.fine("跳过重复媒体文件: type=" + msgtype + " md5=" + md5sum);
                        continue;
                    }
                    uniqueTaskKeys.add(uniqueKey);
    
                    ObjectNode taskJson = objectMapper.createObjectNode();
                    taskJson.put("msgtype", msgtype);
                    taskJson.put("sdkfileid", sdkfileid);
                    taskJson.put("md5sum", md5sum);
                    taskJson.put("fileext", fileext); // 添加 fileext 字段
                    jsonGenerator.writeObject(taskJson);
                    validCount++;
    
                } catch (Exception e) {
                    try {
                        lineNumberReader.reset();
                        String rawLine = lineNumberReader.readLine();
                        logger.severe(String.format("解析失败行 %d | 错误: %s | 原始内容: %s",
                            currentLine, e.getMessage(), rawLine));
                    } catch (IOException ex) {
                        logger.severe("无法读取问题行内容: " + ex.getMessage());
                    }
                    skippedCount++;
                }
            }
    
            jsonGenerator.writeEndArray();
    
            logger.info(String.format(
                "生成媒体文件清单完成\n总记录数: %d\n有效记录: %d\n跳过无效记录: %d\n重复记录: %d",
                totalCount, validCount, skippedCount, duplicateCount
            ));
    
            totalTasks = validCount;
            return validCount;
        } catch (Exception e) {
            logger.severe("生成媒体文件清单失败: " + e.getMessage());
            return -1;
        }
    }
    
    // 新增校验方法
    private static boolean isValidMsgTypeForMedia(String msgtype) {
        if (StringUtils.isBlank(msgtype)) return false;
        switch (msgtype.trim().toLowerCase()) { // 增加空值和大小写处理
            case "image":
            case "voice":
            case "video":
            case "emotion":
            case "file":
                return true;
            default:
                return false;
        }
    }

    private static boolean isValidMD5(String md5sum) {
        return StringUtils.isNotBlank(md5sum) && md5sum.matches("[a-fA-F0-9]{32}");
    }

    private static void executeTaskWithRetry(long sdk, JsonNode taskNode, AtomicInteger successCount, AtomicInteger failureCount) {
        String taskJson = taskNode.toString();
        String errorMsg = "";
        String msgtype = "";
        String sdkfileid = "";
        String fileext = ""; // 新增 fileext 字段
    
        try {
            msgtype = taskNode.path("msgtype").asText();
            sdkfileid = taskNode.path("sdkfileid").asText();
            String md5sum = taskNode.path("md5sum").asText();
            fileext = taskNode.path("fileext").asText(); // 从 JSON 中读取 fileext
    
            // 防御性校验
            if (!isValidMsgTypeForMedia(msgtype) || !isValidSDKFileId(sdkfileid) || !isValidMD5(md5sum)) {
                errorMsg = "Invalid task parameters";
                throw new IllegalArgumentException(errorMsg);
            }
    
            // 下载媒体文件
            File mediaFile = downloadMediaFile(sdk, sdkfileid, md5sum, msgtype, fileext);
            if (mediaFile == null) {
                errorMsg = "Download failed";
                throw new IOException(errorMsg);
            }
    
            // 上传到 S3
            String s3Key = getMediaS3Key(msgtype, md5sum, fileext);
            uploadFileToS3(mediaFile.getAbsolutePath(), mediaS3BucketName, s3Key);
    
            // 清理临时文件
            Files.delete(mediaFile.toPath());
    
            successCount.incrementAndGet();
        } catch (Exception e) {
            failureCount.incrementAndGet();
            errorMsg = String.format("%s: %s", e.getClass().getSimpleName(), e.getMessage());
            logFailureToFile(msgtype, sdkfileid, errorMsg);
        } finally {
            int totalProcessed = successCount.get() + failureCount.get();
            if (totalProcessed % 100 == 0) {
                logProgress(totalProcessed, successCount.get(), failureCount.get());
            }
        }
    }

    private static synchronized void logFailureToFile(String msgtype, String sdkfileid, String error) {
        String logEntry = String.format("[%s] Type: %-6s | FileID: %-20s | Error: %s%n",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                msgtype,
                StringUtils.abbreviate(sdkfileid, 20),
                error);
    
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("logs/media_failures.log", true))) {
            writer.write(logEntry);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "无法写入失败日志文件", e);
        }
    }
    
    // 辅助方法：验证消息类型有效性
    private static boolean isValidMsgType(String msgtype) {
        return Set.of("image", "voice", "video", "emotion", "file").contains(msgtype);
    }

    private static boolean isValidSDKFileId(String sdkfileid) {
        return StringUtils.isNotBlank(sdkfileid) && sdkfileid.length() >= 32;
    }


    private static String decryptData(long sdk, String encryptRandomKey, String encryptChatMsg) {
        try {
            // Base64 解码 encrypt_random_key
            byte[] encryptedKeyBytes = DecryptionUtil.base64Decode(encryptRandomKey);

            // RSA 解密 encrypt_random_key
            String privateKeyStr = KeyConfig.getPrivateKey(); // 从配置文件加载私钥
            String decryptedKey = DecryptionUtil.rsaDecrypt(encryptedKeyBytes, privateKeyStr);

            // 调用 SDK 解密接口
            long msgSlice = Finance.NewSlice();
            int ret = Finance.DecryptData(sdk, decryptedKey, encryptChatMsg, msgSlice);
            if (ret != 0) {
                logger.severe("解密失败, ret: " + ret);
                return null;
            }

            // 获取解密后的消息明文
            String decryptedMsg = Finance.GetContentFromSlice(msgSlice);
            Finance.FreeSlice(msgSlice);

            return decryptedMsg;
        } catch (Exception e) {
            logger.severe("解密失败: " + e.getMessage());
            return null;
        }
    }

    private static String extractContent(JsonNode rootNode, String msgtype) {
        try {
            switch (msgtype) {
                case "text":
                    return rootNode.path("text").path("content").asText();
                case "revoke":
                    return rootNode.path("revoke").path("pre_msgid").asText();
                case "disagree":
                case "agree":
                    return rootNode.path("userid").asText();
                case "card":
                    String corpname = rootNode.path("corpname").asText();
                    String userid = rootNode.path("userid").asText();
                    return corpname + "_" + userid;
                case "location":
                    return rootNode.path("address").asText();
                case "link":
                    return rootNode.path("link_url").asText();
                case "weapp":
                    return rootNode.path("displayname").asText();
                case "image":
                case "voice":
                case "video":
                case "emotion":
                case "file":
                    // 对于媒体类型，返回空字符串或特定字段
                    return rootNode.path("content").asText(); // 假设媒体类型也有 content 字段
                default:
                    // 对于未处理的 msgtype，返回空字符串或默认值
                    return "";
            }
        } catch (Exception e) {
            logger.severe("提取消息内容失败: " + e.getMessage());
            return "";
        }
    }

    private static String extractSeqFileId(JsonNode decryptedRootNode, String msgtype) {
        // 增加sdkfileid有效性校验
        if (!isValidMsgTypeForMedia(msgtype)) return "";
    
        JsonNode msgTypeNode = decryptedRootNode.path(msgtype);
        String sdkfileid = msgTypeNode.path("sdkfileid").asText("").trim();
        
        // 新增校验逻辑
        if (sdkfileid.isEmpty() || sdkfileid.length() < 32) { // 根据实际格式调整校验规则
            logger.warning("Invalid sdkfileid detected: " + sdkfileid);
            return "";
        }
        return sdkfileid;
    }
    
    private static String escapeCsvField(String field) {
        if (field == null || field.isEmpty()) {
            return "\"\""; // 返回空字段的标准表示
        }
        
        StringWriter sw = new StringWriter();
        try (CSVWriter writer = new CSVWriter(sw)) {
            // 使用 OpenCSV 的标准转义逻辑
            writer.writeNext(new String[]{field});
            String escaped = sw.toString().trim();
            
            // 处理 OpenCSV 自动添加的换行符
            if (escaped.endsWith("\n") || escaped.endsWith("\r")) {
                escaped = escaped.substring(0, escaped.length()-1);
            }
            return escaped;
        } catch (IOException e) {
            logger.warning("CSV字段转义失败: " + e.getMessage());
            return "\"" + field.replace("\"", "\"\"") + "\""; // 降级处理
        }
    }

    private static File downloadMediaFile(
        long sdk, 
        String sdkfileid, 
        String md5sum, 
        String msgtype,
        String fileext
    ) throws SdkException {
        // 重试策略参数配置
        final int MAX_RETRIES = 10;
        final long MAX_RETRY_DURATION = 30 * 60 * 1000; // 30分钟
        final long MAX_SLEEP = 30_000; // 30秒
    
        // 任务开始时间
        final long taskStartTime = System.currentTimeMillis();
        int attempt = 0; // 重试次数计数器
        File tempFile = null; // 临时文件
        String indexbuf = ""; // 分片下载索引
    
        try {
            while (true) {
                try {
                    // 创建临时文件
                    try {
                        tempFile = createTempFile(md5sum, msgtype);
                    } catch (IOException e) {
                        throw new SdkException(-1000, "创建临时文件失败: " + e.getMessage(), e);
                    }
    
                    // 创建文件输出流
                    try (FileOutputStream fos = createFileOutputStream(tempFile)) {
                        boolean isFinished = false;
                        while (!isFinished) {
                            long mediaData = Finance.NewMediaData();
                            try {
                                int ret = Finance.GetMediaData(
                                    sdk, 
                                    indexbuf, 
                                    sdkfileid, 
                                    null, 
                                    null, 
                                    10, 
                                    mediaData
                                );
    
                                if (ret != 0) {
                                    throw new SdkException(ret, "GetMediaData failed, ret=" + ret + " sdkfileid=" + sdkfileid);
                                }
    
                                byte[] data = Finance.GetData(mediaData);
                                if (data != null && data.length > 0) {
                                    try {
                                        fos.write(data);
                                        fos.flush();
                                    } catch (IOException e) {
                                        logger.severe("写入文件失败: " + e.getMessage());
                                        throw new SdkException(-1002, "写入文件失败: " + e.getMessage(), e);
                                    }
                                }
    
                                isFinished = Finance.IsMediaDataFinish(mediaData) == 1;
                                if (!isFinished) {
                                    indexbuf = Finance.GetOutIndexBuf(mediaData);
                                }
                            } finally {
                                Finance.FreeMediaData(mediaData);
                            }
                        }
    
                        // 重命名临时文件
                        File finalFile = buildFinalFile(tempFile, md5sum, msgtype, fileext);
                        try {
                            Files.move(tempFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException e) {
                            logger.severe("移动文件失败: " + e.getMessage());
                            throw new SdkException(-1003, "移动文件失败: " + e.getMessage(), e);
                        }
    
                        // 记录成功日志
                        long totalTime = System.currentTimeMillis() - taskStartTime;
                        double fileSizeMB = finalFile.length() / (1024.0 * 1024.0);
                        if (attempt > 0) { // 仅在重试次数>0时记录
                            logger.info(String.format(
                                "[媒体下载重试成功] 文件:%s 类型:%-6s 重试次数:%d 总耗时:%.1fs 文件大小:%.2fMB",
                                md5sum, msgtype, attempt, totalTime / 1000.0, fileSizeMB
                            ));
                        }
    
                        return finalFile;
                    } catch (IOException e) {
                        logger.severe("文件输出流关闭失败: " + e.getMessage());
                        throw new SdkException(-1004, "文件输出流关闭失败: " + e.getMessage(), e);
                    }
                } catch (SdkException e) {
                    // 非重试性错误直接抛出
                    if (!isRetryableError(e.getStatusCode())) {
                        throw e;
                    }
    
                    // 检查超时
                    long elapsed = System.currentTimeMillis() - taskStartTime;
                    if (elapsed > MAX_RETRY_DURATION) {
                        // 记录失败日志
                        String errorReason = classifyError(e.getStatusCode());
                        logger.severe(String.format(
                            "[媒体下载重试失败] 文件:%s 类型:%-6s 错误码:%d 重试次数:%d 累计耗时:%.1fs 错误原因:%s",
                            md5sum, msgtype, e.getStatusCode(), attempt, elapsed / 1000.0, errorReason
                        ));
                        throw new SdkException(e.getStatusCode(), 
                            "超过最大重试时间(" + MAX_RETRY_DURATION / 60000 + "分钟)" +
                            " last_error=" + e.getMessage(),
                            e
                        );
                    }
    
                    // 检查重试次数
                    if (attempt >= MAX_RETRIES) {
                        // 记录失败日志
                        String errorReason = classifyError(e.getStatusCode());
                        logger.severe(String.format(
                            "[媒体下载重试失败] 文件:%s 类型:%-6s 错误码:%d 重试次数:%d 累计耗时:%.1fs 错误原因:%s",
                            md5sum, msgtype, e.getStatusCode(), attempt, elapsed / 1000.0, errorReason
                        ));
                        throw new SdkException(e.getStatusCode(), 
                            "超过最大重试次数(" + MAX_RETRIES + ")" +
                            " last_error=" + e.getMessage(),
                            e
                        );
                    }
    
                    // 计算退避时间
                    long baseDelay = (long) Math.pow(2, attempt) * 1000;
                    long jitter = ThreadLocalRandom.current().nextLong(500, 1500);
                    long sleepTime = Math.min(baseDelay + jitter, MAX_SLEEP);
    
                    // 记录重试日志
                    logger.warning(String.format(
                        "[媒体下载重试] 文件:%s 类型:%-6s 第%02d次重试 错误码:%d 休眠:%.1fs 累计耗时:%.1fs",
                        md5sum, msgtype, attempt + 1,
                        e.getStatusCode(),
                        sleepTime / 1000.0,
                        elapsed / 1000.0
                    ));
    
                    // 休眠处理
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SdkException(-1, "下载任务被中断", ie);
                    }
    
                    attempt++;
                    cleanTempFile(tempFile);
                    tempFile = null;
                }
            }
        } finally {
            cleanTempFile(tempFile);
        }
    }

    private static String classifyError(int statusCode) {
        switch (statusCode) {
            case 10001: return "网络波动";
            case 10002: return "数据解析失败";
            case -1000: return "临时文件错误";
            case -1001: return "文件IO异常";
            default:    return "未知错误(" + statusCode + ")";
        }
    }

    private static FileOutputStream createFileOutputStream(File file) throws SdkException {
        try {
            return new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new SdkException(-1001, "文件未找到: " + file.getAbsolutePath(), e);
        }
    }

    private static boolean isRetryableError(int statusCode) {
        return statusCode == 10001 || statusCode == 10002 || statusCode == -1000 || statusCode == -1001;
    }

    private static File createTempFile(String md5sum, String msgtype) throws IOException {
        String prefix = String.format("%s_%s_", md5sum, msgtype);
        return Files.createTempFile(prefix, ".tmp").toFile();
    }

    private static File buildFinalFile(File tempFile, String md5sum, String msgtype, String fileext) {
        String ext = getFileExtension(msgtype, fileext);
        String fileName = md5sum + ext;
        return new File(tempFile.getParent(), fileName);
    }

    private static String getFileExtension(String msgtype, String fileext) {
        switch (msgtype.toLowerCase()) {
            case "image": return ".jpg";
            case "voice": return ".mp3";
            case "video": return ".mp4";
            case "emotion": return ".gif";
            case "file":  return "." + fileext; // 使用 fileext 作为扩展名
            default:      return ".dat";
        }
    }

    private static void cleanTempFile(File file) {
        if (file != null && file.exists()) {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                logger.warning("清理临时文件失败: " + file.getAbsolutePath());
            }
        }
    }

    private static String generateMediaFileName(String md5sum, String msgtype, String fileext) {
        String extension = "";
        switch (msgtype) {
            case "image":
                extension = ".jpg";
                break;
            case "voice":
                extension = ".mp3";
                break;
            case "video":
                extension = ".mp4";
                break;
            case "emotion":
                extension = ".gif";
                break;
            case "file":
                extension = "." + fileext; // 使用 fileext 作为扩展名
                break;
            default:
                extension = ".dat";
        }
        return md5sum + extension;
    }

    private static String getMediaS3Key(String msgtype, String md5sum, String fileext) {
        String safeMsgType = isValidMsgType(msgtype) ? msgtype : "unknown";
        return String.format("raw/wecom_chat/media/%s/%s",
            safeMsgType,
            generateMediaFileName(md5sum, msgtype, fileext)
        );
    }
            
    private static boolean downloadMediaFilesToS3(long sdk) {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        String mediaFilesPath = curatedFilePath.replace("chat_", "media_files_").replace(".csv", ".json");
    
        // 初始化日志目录
        new File("logs").mkdirs();
    
        // 生成媒体清单
        totalTasks = generateMediaFilesJSON(curatedFilePath, mediaFilesPath);
        if (totalTasks < 0) {
            logger.severe("媒体清单生成失败");
            return false;
        }
    
        // 处理任务
        processMediaTasksStreaming(mediaFilesPath, sdk, successCount, failureCount);
        boolean finalStatus = waitForCompletion(totalTasks, successCount, failureCount);
    
        // 强制打印最终进度
        logProgress(totalTasks, successCount.get(), failureCount.get());
        
        return finalStatus;
    }

     private static void processMediaTasksStreaming(
        String mediaFilesPath, 
        long sdk, 
        AtomicInteger successCount, 
        AtomicInteger failureCount
    ) {
        try (InputStream inputStream = Files.newInputStream(Paths.get(mediaFilesPath));
             JsonParser parser = objectMapper.getFactory().createParser(inputStream)) {
    
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                logger.severe("媒体清单文件格式错误");
                return;
            }
    
            List<JsonNode> batch = new ArrayList<>(1000);
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                JsonNode taskNode = objectMapper.readTree(parser);
                batch.add(taskNode);
    
                if (batch.size() >= 1000) {
                    submitBatchTasks(batch, sdk, successCount, failureCount);
                    batch.clear();
                }
            }
    
            // 提交剩余任务
            if (!batch.isEmpty()) {
                submitBatchTasks(batch, sdk, successCount, failureCount);
            }
    
        } catch (Exception e) {
            logger.severe("流式处理媒体任务失败: " + e.getMessage());
        }
    }
    
    private static void submitBatchTasks(
        List<JsonNode> batch, 
        long sdk, 
        AtomicInteger successCount, 
        AtomicInteger failureCount
    ) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) networkRetryExecutor;
        for (JsonNode taskNode : batch) {
            // 阻塞直到队列有空位
            while (executor.getQueue().remainingCapacity() == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.severe("任务提交被中断");
                    return;
                }
            }
            executor.submit(() -> executeTaskWithRetry(sdk, taskNode, successCount, failureCount));
        }
    }

    private static boolean waitForCompletion(int totalTasks, AtomicInteger successCount, AtomicInteger failureCount) {
        final long STATUS_INTERVAL = 60_000; // 每分钟强制打印进度
        long lastStatusTime = System.currentTimeMillis();
    
        while (true) {
            int completed = successCount.get() + failureCount.get();
            long currentTime = System.currentTimeMillis();
    
            // 强制每分钟至少打印一次进度
            if (currentTime - lastStatusTime > STATUS_INTERVAL) {
                logProgress(completed, successCount.get(), failureCount.get());
                lastStatusTime = currentTime;
            }
    
            // 完成条件（必须全部完成）
            if (completed >= totalTasks) {
                logger.info(String.format(
                    "【最终统计】总数: %d | 成功: %d (%.2f%%) | 失败: %d (%.2f%%)",
                    totalTasks,
                    successCount.get(), (successCount.get() * 100.0 / totalTasks),
                    failureCount.get(), (failureCount.get() * 100.0 / totalTasks)
                ));
                return true;
            }
    
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.severe("进度监控被中断");
                return true;
            }
        }
    }

    private static void logProgress(int completed, int success, int failure) {
        double progress = (completed * 100.0) / totalTasks;
        String progressBar = buildProgressBar(progress);
        
        logger.info(String.format(
            "媒体下载进度: %s %d/%d (%.2f%%) | 成功=%d | 失败=%d",
            progressBar, completed, totalTasks, progress, success, failure
        ));
    }
    
    private static String buildProgressBar(double progress) {
        int bars = (int) (progress / 2); // 50个字符表示100%
        return String.format("[%-50s]", StringUtils.repeat("#", bars));
    }

    private static final ExecutorService networkRetryExecutor = Executors.newFixedThreadPool(10); // 限制并发度为 10

    private static boolean compressAndUploadFilesToS3() {
        try {
            // 压缩并上传 raw 文件
            String rawZipFilePath = compressFile(rawFilePath, "wecom_chat_" + taskDateStr + ".zip");
            String rawS3Key = "home/wecom/inbound/c360/chat/archival/" + taskDateStr + "/wecom_chat_" + taskDateStr + ".zip";
            logger.info("开始上传 raw 文件到 S3: " + rawS3Key);
            uploadFileToS3(rawZipFilePath, s3BucketName, rawS3Key);
            logger.info("raw 文件上传完成: " + rawS3Key);
    
            // 压缩并上传 curated 文件
            String curatedZipFilePath = compressFile(curatedFilePath, "chat_" + taskDateStr + ".zip");
            String curatedS3Key = "home/wecom/inbound/c360/chat/" + taskDateStr + "/chat_" + taskDateStr + ".zip";
            logger.info("开始上传 curated 文件到 S3: " + curatedS3Key);
            uploadFileToS3(curatedZipFilePath, s3BucketName, curatedS3Key);
            logger.info("curated 文件上传完成: " + curatedS3Key);
    
            // 生成 userid_mapping_yyyymmdd.csv 文件的路径
            // String mappingFilePath = curatedFilePath.replace("chat_", "userid_mapping_");
    
            // 压缩 userid_mapping_yyyymmdd.csv 文件
            // String mappingZipFilePath = compressFile(mappingFilePath, "userid_mapping_" + taskDateStr + ".zip");
            // String mappingS3Key = "home/wecom/inbound/c360/chat/" + taskDateStr + "/userid_mapping_" + taskDateStr + ".zip";
            // logger.info("开始上传 userid_mapping 文件到 S3: " + mappingS3Key);
            // (mappingZipFilePath, s3BucketName, mappingS3Key);
            // logger.info("userid_mapping 文件上传完成: " + mappingS3Key);
    
            // 删除本地 raw 和 curated 目录下的所有子文件夹和文件
            deleteDirectoryContents("/home/ec2-user/wecom_integration/data/raw"); // 清空 raw 目录下的内容
            deleteDirectoryContents("/home/ec2-user/wecom_integration/data/curated"); // 清空 curated 目录下的内容
    
            return true;
        } catch (Exception e) {
            logger.severe("压缩并上传文件到 S3 失败: " + e.getMessage());
            return false;
        }
    }

   public static void deleteDirectoryContents(String dirPath) {
        deleteDirContents(new File(dirPath));
    }
    
    /**
     * 递归删除指定目录下的所有内容（保留目录本身）
     * @param directory 要清理的目录
     */
    private static void deleteDirContents(File directory) {
        if (!directory.exists() || !directory.isDirectory()) {
            logger.warning("目录不存在或不是有效目录: " + directory.getAbsolutePath());
            return;
        }
    
        try {
            Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    logger.fine("已删除文件: " + file);
                    return FileVisitResult.CONTINUE;
                }
            
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (!dir.equals(directory.toPath())) {
                        Files.delete(dir);
                        logger.info("已删除目录: " + dir);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.log(Level.SEVERE, "清理目录失败: " + directory.getAbsolutePath(), e);
        }
    }

    private static String compressFile(String filePath, String zipFileName) throws IOException {
        File file = new File(filePath);
        String zipFilePath = file.getParent() + "/" + zipFileName;

        try (FileOutputStream fos = new FileOutputStream(zipFilePath);
             ZipOutputStream zipOut = new ZipOutputStream(fos);
             FileInputStream fis = new FileInputStream(file)) {

            ZipEntry zipEntry = new ZipEntry(file.getName());
            zipOut.putNextEntry(zipEntry);

            byte[] bytes = new byte[1024];
            int length;
            while ((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
        }

        logger.info("文件已压缩为 ZIP: " + zipFilePath);
        return zipFilePath;
    }

    private static void uploadFileToS3(String filePath, String s3BucketName, String s3Key) {
        final int MAX_RETRIES = 3;
        int attempt = 0;
    
        while (attempt <= MAX_RETRIES) {
            try {
                s3Client.putObject(
                    PutObjectRequest.builder()
                        .bucket(s3BucketName)
                        .key(s3Key)
                        .build(),
                    Paths.get(filePath)
                );
                return;
            } catch (S3Exception e) {
                if (e.statusCode() == 503 || e.awsErrorDetails().errorCode().contains("SlowDown")) {
                    attempt++;
                    long delay = calculateExponentialBackoff(attempt);
                    logger.warning("触发速率限制，第 " + attempt + " 次重试，延迟 " + delay + "ms");
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.severe("上传线程被中断: " + ie.getMessage());
                        throw new RuntimeException("上传任务被中断", ie);
                    }
                } else {
                    logger.severe("S3上传致命错误: " + e.awsErrorDetails().errorMessage());
                    throw e;
                }
            } catch (Exception e) {
                logger.severe("未知上传错误: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("上传失败，超过最大重试次数");
    }

    private static long calculateExponentialBackoff(int attempt) {
        long baseDelay = 1000; // 1秒
        long maxDelay = 10000; // 10秒
        long delay = (long) (baseDelay * Math.pow(2, attempt));
        return Math.min(delay, maxDelay) + new Random().nextInt(1000); // 添加随机抖动
    }
}
