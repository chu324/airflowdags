package com.tencent.wework;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryOnStatusCodeCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.opencsv.CSVReader;
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
import java.time.ZonedDateTime;
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
    private static final String s3BucketName = "175826060701-tprdevsftp-sftp-dev-cn-north-1"; // raw 和 curated 文件
    private static final String mediaS3BucketName = "175826060701-eds-qa-cn-north-1"; // 媒体文件

    // 定义 raw 文件路径
    private static String rawFilePath = null;

    // 定义 curated 文件路径
    private static String curatedFilePath = null;

    private static final String PENDING_TASKS_FILE = "pending_media_tasks.log";

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
    
        // 生成 media_files.csv 文件
        String mediaFilesPath = curatedFilePath.replace("chat_", "media_files_");
        boolean mediaFilesGenerated = generateMediaFilesCSV(curatedFilePath, mediaFilesPath);
        if (!mediaFilesGenerated) {
            logger.severe("生成 media_files.csv 文件失败");
        }
    
        // 生成 userid_mapping_yyyymmdd.csv 文件,保留后续使用
        //String mappingFilePath = curatedFilePath.replace("chat_", "userid_mapping_");
        //generateUserIdMappingFile(curatedFilePath, mappingFilePath);
    
        // 阶段 3: 下载媒体文件到 S3
        boolean mediaDownloadSuccess = downloadMediaFilesToS3(sdk);
        if (!mediaDownloadSuccess) {
            logger.severe("部分媒体文件下载失败，但仍将继续上传 raw 和 curated 文件到 S3");
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
                    if (ret == 10001) {
                        if (!isInRetryPeriod) {
                            isInRetryPeriod = true;
                            retryStartTime = System.currentTimeMillis();
                            logger.info("检测到 ret 10001，开始记录波动时间，retryStartTime: " + retryStartTime);
                        }
                        long elapsedTime = System.currentTimeMillis() - retryStartTime;
                        if (elapsedTime < MAX_RETRY_TIME) {
                            Thread.sleep(RETRY_INTERVAL); // 可能抛出 InterruptedException
                            continue;
                        } else {
                            logger.severe("GetChatData failed, ret: " + ret + ". 当前波动超过最大重试时间.");
                            sendSNSErrorMessage("GetChatData 持续失败，ret=" + ret + "，当前波动超过最大重试时间。");
                            return false;
                        }
                    } else {
                        logger.severe("GetChatData failed, ret: " + ret + ". 任务中断。");
                        sendSNSErrorMessage("GetChatData 失败，ret=" + ret + "，任务中断。");
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
    
            // 添加表头（新增 md5sum 列）
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
                    JsonNode decryptedRootNode = objectMapper.readTree(escapedDecryptedMsg);
    
                    String action = decryptedRootNode.path("action").asText();
                    String sender = decryptedRootNode.path("from").asText();
                    JsonNode tolistNode = decryptedRootNode.path("tolist");
                    String roomid = decryptedRootNode.path("roomid").asText();
                    long msgtime = decryptedRootNode.path("msgtime").asLong();
                    String msgtype = decryptedRootNode.path("msgtype").asText();
                    String sdkfileid = extractSeqFileId(decryptedRootNode, msgtype);
                    String md5sum = extractMd5Sum(decryptedRootNode, msgtype); // 新增 md5sum 提取
    
                    JsonNode rawJsonNode = decryptedRootNode.path(msgtype);
                    String rawJson = rawJsonNode.toString();
    
                    Instant instant = Instant.ofEpochMilli(msgtime);
                    ZonedDateTime beijingTime = instant.atZone(ZoneId.of("Asia/Shanghai"));
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String beijingTimeStr = beijingTime.format(formatter);
    
                    String escapedRawJson = escapeCsvField(rawJson);
    
                    // 写入 curated 文件（新增 md5sum 列）
                    if (tolistNode.isArray()) {
                        for (JsonNode to : tolistNode) {
                            String receiver = to.asText();
                            curatedWriter.write(seqStr + "," + msgid + "," + action + "," + sender + "," + receiver + "," + roomid + "," + beijingTimeStr + "," + msgtype + "," + sdkfileid + "," + md5sum + "," + escapedRawJson);
                            curatedWriter.newLine();
                        }
                    } else {
                        String receiver = tolistNode.toString();
                        curatedWriter.write(seqStr + "," + msgid + "," + action + "," + sender + "," + receiver + "," + roomid + "," + beijingTimeStr + "," + msgtype + "," + sdkfileid + "," + md5sum + "," + escapedRawJson);
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

    private static boolean generateMediaFilesCSV(String chatFilePath, String mediaFilesPath) {
        final int REQUIRED_COLUMNS = 11;
        Set<String> processedMd5Sums = new HashSet<>();
        int totalRecords = 0;
    
        try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(chatFilePath))
                .withCSVParser(new CSVParserBuilder()
                        .withQuoteChar('"')  // 确保引号字符正确
                        .withEscapeChar('\\')  // 使用反斜杠作为转义字符
                        .build())
                .build();
             CSVWriter csvWriter = new CSVWriter(new FileWriter(mediaFilesPath))) {
    
            // 写入表头
            csvWriter.writeNext(new String[]{"msgtype", "sdkfileid", "md5sum"});
    
            String[] record;
            while ((record = csvReader.readNext()) != null) {
                totalRecords++;
                if (record.length < REQUIRED_COLUMNS) {
                    logger.warning("记录列数不足: 实际列数=" + record.length);
                    continue;
                }
    
                String msgtype = record[7].trim();
                String sdkfileid = record[8].trim();
                String md5sum = record[9].trim();
    
                if (!isValidMsgType(msgtype)) {
                    logger.fine("跳过非媒体类型: " + msgtype);
                    continue;
                }
    
                if (StringUtils.isBlank(sdkfileid) || StringUtils.isBlank(md5sum)) {
                    logger.warning("字段为空: msgtype=" + msgtype);
                    continue;
                }
    
                if (processedMd5Sums.add(md5sum)) {
                    // 将写入操作移到循环内部
                    csvWriter.writeNext(new String[]{msgtype, escapeCsvField(sdkfileid), escapeCsvField(md5sum)});
                }
            }
    
            logger.info(String.format("生成媒体文件清单: 总记录=%d, 去重后记录=%d", totalRecords, processedMd5Sums.size()));
            return true;
        } catch (CsvValidationException e) {
            logger.severe("CSV 文件格式错误: " + e.getMessage());
            return false;
        } catch (Exception e) {
            logger.severe("生成媒体文件清单失败: " + e.getMessage());
            return false;
        }
    }
    
    // 辅助方法：验证消息类型有效性
    private static boolean isValidMsgType(String msgtype) {
        return Set.of("image", "voice", "video", "emotion", "file").contains(msgtype);
    }

    private static boolean isValidSDKFileId(String sdkfileid) {
        return !StringUtils.isBlank(sdkfileid);
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

    private static String extractSeqFileId(JsonNode rootNode, String msgtype) {
        // 定义包含 sdkfileid 的有效 msgtype 集合
        Set<String> validMsgTypes = new HashSet<>(Arrays.asList(
            "image", "voice", "video", "emotion", "file", "meeting_voice_call", "voip_doc_share"
        ));

        try {
            // 如果 msgtype 不在有效集合中，直接返回空字符串
            if (!validMsgTypes.contains(msgtype)) {
                return "";
            }

            // 检查 msgtype 对应的节点是否存在
            if (rootNode.has(msgtype)) {
                JsonNode msgTypeNode = rootNode.path(msgtype);
                // 检查 sdkfileid 字段是否存在且不为空
                if (msgTypeNode.has("sdkfileid")) {
                    String sdkfileid = msgTypeNode.path("sdkfileid").asText();
                    if (sdkfileid != null && !sdkfileid.isEmpty()) {
                        return sdkfileid;
                    } else {
                        logger.info("sdkfileid 字段为空: msgtype=" + msgtype);
                    }
                } else {
                    logger.info("sdkfileid 字段缺失: msgtype=" + msgtype);
                }
            } else {
                logger.info("msgtype 节点缺失: msgtype=" + msgtype);
            }
            // 如果 msgtype 节点或 sdkfileid 字段不存在，返回空字符串
            return "";
        } catch (Exception e) {
            logger.severe("提取 sdkfileid 失败: " + e.getMessage());
            return "";
        }
    }

    private static String escapeCsvField(String field) {
        if (field == null) {
            return "\"\"";
        }
        // 转义双引号
        String escapedField = field.replace("\"", "\"\"");
        // 用双引号包裹字段
        return "\"" + escapedField + "\"";
    }

    private static void downloadMediaFile(long sdk, String sdkfileid, String md5sum, String msgtype) {
        String indexbuf = "";
        File tempFile = null;
    
        try {
            tempFile = File.createTempFile(md5sum + "_", ".tmp");
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                boolean isFinished = false;
                while (!isFinished) {
                    long mediaData = Finance.NewMediaData();
                    try {
                        int ret = Finance.GetMediaData(sdk, indexbuf, sdkfileid, null, null, 10, mediaData);
                        if (ret != 0) {
                            throw new SdkException(ret, "GetMediaData failed, ret=" + ret);
                        }
    
                        byte[] data = Finance.GetData(mediaData);
                        fos.write(data);
                        isFinished = Finance.IsMediaDataFinish(mediaData) == 1;
                        if (!isFinished) {
                            indexbuf = Finance.GetOutIndexBuf(mediaData);
                        }
                    } finally {
                        Finance.FreeMediaData(mediaData);
                    }
                }
            }
    
            // 生成最终文件名并重命名临时文件
            String finalFileName = generateMediaFileName(md5sum, msgtype);
            File finalFile = new File(tempFile.getParent(), finalFileName);
            Files.move(tempFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    
            // 上传到S3
            String s3Key = getMediaS3Key(msgtype, md5sum);
            uploadFileToS3(finalFile.getAbsolutePath(), mediaS3BucketName, s3Key);
        } catch (Exception e) {
            throw new RuntimeException("媒体文件处理失败", e);
        } finally {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
        }
    }

    private static String generateMediaFileName(String md5sum, String msgtype) {
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
                extension = ".bin";
                break;
            default:
                extension = ".dat";
        }
        return md5sum + extension;
    }

    private static String getMediaS3Key(String msgtype, String md5sum) {
        String safeMsgType = isValidMsgType(msgtype) ? msgtype : "unknown";
        return String.format("raw/wecom_chat/media/%s/%s",
            safeMsgType,
            generateMediaFileName(md5sum, msgtype)
        );
    }
            
    /**
     * 生成媒体文件S3存储路径
     * @param msgtype  消息类型
     * @param md5sum   文件唯一标识（不再使用sdkfileid）
     * @return S3完整路径
     */
    private static boolean downloadMediaFilesToS3(long sdk) {
        // 初始化原子计数器
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        // 加载历史未完成任务
        loadPendingTasks(sdk, successCount, failureCount);
    
        String mediaFilesPath = curatedFilePath.replace("chat_", "media_files_");
        logger.info("开始处理媒体文件下载任务，文件清单路径: " + mediaFilesPath);
    
        // 检查媒体清单文件有效性
        if (!validateMediaFilesCSV(mediaFilesPath)) {
            logger.severe("媒体文件清单格式错误或不存在");
            return false;
        }
    
        // 加载所有媒体记录（包含历史未完成任务）
        List<String> allTasks = new ArrayList<>(loadAllMediaRecords(mediaFilesPath));
        int totalTasks = allTasks.size();
        logger.info("总媒体文件下载任务数: " + totalTasks + " (含历史未完成任务)");
    
        // 进度监控线程（每分钟打印）
        ScheduledExecutorService progressScheduler = Executors.newSingleThreadScheduledExecutor();
        progressScheduler.scheduleAtFixedRate(() -> {
            int completed = successCount.get() + failureCount.get();
            logger.info(String.format(
                "[下载进度] 总任务=%d, 已完成=%d (成功=%d 失败=%d), 活跃线程=%d, 队列积压=%d",
                totalTasks, completed, successCount.get(), failureCount.get(),
                ((ThreadPoolExecutor)networkRetryExecutor).getActiveCount(),
                ((ThreadPoolExecutor)networkRetryExecutor).getQueue().size()
            ));
        }, 1, 1, TimeUnit.MINUTES);
    
        // 提交所有下载任务
        for (String taskId : allTasks) {
            networkRetryExecutor.submit(() -> {
                try {
                    executeTaskWithRetry(sdk, taskId, successCount, failureCount);
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    logger.severe("任务提交失败: " + taskId + " - " + e.getMessage());
                    savePendingTask(taskId); // 持久化失败任务
                }
            });
        }
    
        // 等待所有任务完成（带超时机制）
        boolean finalStatus = waitForCompletion(totalTasks, successCount, failureCount);
        
        // 关闭资源
        progressScheduler.shutdown();
        networkRetryExecutor.shutdown();
        
        return finalStatus;
    }

    private static boolean validateMediaFilesCSV(String mediaFilesPath) {
        try (CSVReader csvReader = new CSVReader(new FileReader(mediaFilesPath))) {
            String[] header = csvReader.readNext();
            return header != null && header.length >= 3 
                && header[0].equals("msgtype") 
                && header[1].equals("sdkfileid")
                && header[2].equals("md5sum");
        } catch (Exception e) {
            logger.severe("验证媒体文件清单失败: " + e.getMessage());
            return false;
        }
    }

    private static boolean waitForCompletion(int totalTasks, AtomicInteger successCount, AtomicInteger failureCount) {
        final long GLOBAL_TIMEOUT = 48 * 60 * 60 * 1000; // 48小时全局超时
        final long startTime = System.currentTimeMillis();
        
        while (true) {
            int completed = successCount.get() + failureCount.get();
            
            // 完成条件判断
            if (completed >= totalTasks) {
                logger.info("所有任务处理完成");
                return failureCount.get() == 0;
            }
            
            // 超时处理
            if (System.currentTimeMillis() - startTime > GLOBAL_TIMEOUT) {
                logger.severe("全局超时（48小时），剩余任务: " + (totalTasks - completed));
                saveRemainingTasks(); // 持久化剩余任务
                return false;
            }
            
            // 休眠检测间隔
            try {
                Thread.sleep(30000); // 每30秒检测一次
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.severe("进度监控被中断");
                return false;
            }
        }
    }

    private static synchronized void saveRemainingTasks() {
        try {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) networkRetryExecutor;
            BlockingQueue<Runnable> queue = executor.getQueue();
            
            List<String> remainingTasks = new ArrayList<>();
            for (Runnable task : queue) {
                if (task instanceof FutureTask) {
                    // 通过反射获取任务标识（实际项目建议使用包装类）
                    Field callableField = FutureTask.class.getDeclaredField("callable");
                    callableField.setAccessible(true);
                    Object callable = callableField.get(task);
                    
                    if (callable instanceof Callable) {
                        // 假设任务标识存储在callable中
                        remainingTasks.add(callable.toString()); 
                    }
                }
            }
            
            if (!remainingTasks.isEmpty()) {
                Files.write(Paths.get(PENDING_TASKS_FILE), 
                    remainingTasks, 
                    StandardOpenOption.CREATE, 
                    StandardOpenOption.TRUNCATE_EXISTING);
            }
        } catch (Exception e) {
            logger.severe("持久化剩余任务失败: " + e.getMessage());
        }
    }


    private static synchronized void savePendingTask(String taskId) {
        try {
            Files.write(Paths.get(PENDING_TASKS_FILE), 
                (taskId + "\n").getBytes(), 
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.severe("保存待处理任务失败: " + e.getMessage());
        }
    }

    private static synchronized void removePendingTask(String taskId) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(PENDING_TASKS_FILE));
            lines.removeIf(line -> line.equals(taskId));
            Files.write(Paths.get(PENDING_TASKS_FILE), lines);
        } catch (IOException e) {
            logger.severe("移除已完成任务失败: " + e.getMessage());
        }
    }

    private static void loadPendingTasks(long sdk, AtomicInteger successCount, AtomicInteger failureCount) {
        // 修改后的方法实现
        if (!Files.exists(Paths.get(PENDING_TASKS_FILE))) return;
        
        try {
            List<String> tasks = Files.readAllLines(Paths.get(PENDING_TASKS_FILE));
            logger.info("发现未完成任务数: " + tasks.size());
            
            for (String taskId : tasks) {
                networkRetryExecutor.submit(() -> {
                    executeTaskWithRetry(sdk, taskId, successCount, failureCount);
                });
            }
            Files.delete(Paths.get(PENDING_TASKS_FILE));
        } catch (IOException e) {
            logger.severe("加载待处理任务失败: " + e.getMessage());
        }
    }

    /**
     * 加载所有媒体文件记录
     */
    private static List<String> loadAllMediaRecords(String mediaFilesPath) {
        List<String> allTasks = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(mediaFilesPath))) {
            csvReader.readNext(); // 跳过表头
            String[] record;
            while ((record = csvReader.readNext()) != null) {
                if (record.length < 3) continue;
                String taskId = String.join("|", record[0], record[1], record[2]); // 格式: msgtype|sdkfileid|md5sum
                allTasks.add(taskId);
            }
        } catch (Exception e) {
            logger.severe("加载媒体文件清单失败: " + e.getMessage());
        }
        return allTasks;
    }

    /**
     * 将任务列表分批次
     */
    private static List<List<String>> batchMediaRecords(List<String> allTasks, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        List<String> currentBatch = new ArrayList<>();
        for (String task : allTasks) {
            currentBatch.add(task);
            if (currentBatch.size() >= batchSize) {
                batches.add(currentBatch);
                currentBatch = new ArrayList<>();
            }
        }
        if (!currentBatch.isEmpty()) batches.add(currentBatch);
        return batches;
    }

    private static final ExecutorService networkRetryExecutor = 
        Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);  // 设为守护线程
            t.setName("NetworkRetryThread-" + t.getId());
            return t;
        });

    private static boolean isNetworkAvailable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("qyapi.weixin.qq.com", 443), 5000);
            return true;
        } catch (IOException e) {
            logger.warning("网络不可达，错误: " + e.getMessage());
            return false;
        }
    }
        
    /**
     * 带重试的任务执行
     */
    private static void executeTaskWithRetry(long sdk, String taskId, 
            AtomicInteger successCount, AtomicInteger failureCount) {
        String[] parts = taskId.split("\\|");
        String msgtype = parts[0], sdkfileid = parts[1], md5sum = parts[2];
        int retryCount = 0;
        long baseDelayMs = 1000;
    
        while (true) {
            try {
                // 网络可达性检测
                while (!isNetworkAvailable()) {
                    logger.warning("网络不可用[" + taskId + "] 等待恢复...");
                    Thread.sleep(60000);
                }
    
                logger.info("开始下载: " + taskId + " (重试次数=" + retryCount + ")");
                downloadMediaFile(sdk, sdkfileid, md5sum, msgtype);
                
                successCount.incrementAndGet();
                removePendingTask(taskId);
                logger.info("下载成功: " + taskId);
                return;
                
            } catch (SdkException e) {
                handleSdkException(e, taskId, retryCount, baseDelayMs);
                retryCount++;
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.severe("不可恢复错误: " + taskId + " - " + e.getMessage());
                saveFailedRecord(taskId, "FATAL_ERROR", -1);
                return;
            }
        }
    }

    private static void handleSdkException(SdkException e, String taskId, 
            int retryCount, long baseDelayMs) throws InterruptedException {
        if (e.getStatusCode() == 10001) { // 网络错误
            long delay = calculateBackoffDelay(retryCount, baseDelayMs);
            logger.warning("网络波动[" + taskId + "] 等待 " + delay/1000 + "秒后重试...");
            Thread.sleep(delay); // 这里可能会抛出 InterruptedException
        } else { // 其他错误
            logger.severe("SDK错误[" + taskId + "] 代码=" + e.getStatusCode());
            if (retryCount >= 3) {
                throw new RuntimeException("超过最大重试次数");
            }
            Thread.sleep(5000);
        }
    }
    private static long calculateBackoffDelay(int retryCount, long baseDelayMs) {
        long maxDelay = 10 * 60 * 1000; // 10分钟上限
        long delay = (long) (baseDelayMs * Math.pow(2, retryCount));
        return Math.min(delay, maxDelay);
    }

    private static boolean compressAndUploadFilesToS3() {
        try {
            // 压缩并上传 raw 文件
            String rawZipFilePath = compressFile(rawFilePath, "wecom_chat_" + taskDateStr + ".zip");
            String rawS3Key = "home/wecom/inbound/c360/chat/" + taskDateStr + "/wecom_chat_" + taskDateStr + ".zip";
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
            // uploadFileToS3(mappingZipFilePath, s3BucketName, mappingS3Key);
            // logger.info("userid_mapping 文件上传完成: " + mappingS3Key);
    
            // 上传 failed_records 文件到 S3
            String failedRecordsFilePath = curatedFilePath.replace("chat_", "failed_records_");
            if (new File(failedRecordsFilePath).exists()) {
                String failedRecordsS3Key = "rejected/wecom_chat/" + taskDateStr + "/failed_records_" + taskDateStr + ".csv";
                logger.info("开始上传 failed_records 文件到 S3: " + failedRecordsS3Key);
                uploadFileToS3(failedRecordsFilePath, "175826060701-eds-qa-cn-north-1", failedRecordsS3Key);
                logger.info("failed_records 文件上传完成: " + failedRecordsS3Key);
            }
    
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
        // 创建凭证提供者
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

        // 构建 S3 客户端
        S3Client s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider) // 使用凭证提供者
            .region(Region.CN_NORTH_1) // 确保区域正确
            .overrideConfiguration(config -> config.retryPolicy(
                RetryPolicy.builder()
                    .numRetries(5) // 最大重试次数
                    .retryCondition(RetryOnStatusCodeCondition.create(403, 500)) // 在 403 或 500 时重试
                    .build()
            ))
            .build();

        try {
            // 上传文件
            s3Client.putObject(PutObjectRequest.builder()
                .bucket(s3BucketName)
                .key(s3Key)
                .build(), Paths.get(filePath));
        } catch (S3Exception e) {
            // 失败时记录日志
            logger.severe("上传文件到 S3 失败: " + e.awsErrorDetails().errorMessage());
        } finally {
            s3Client.close();
        }
    }

    private static void sendSNSErrorMessage(String message) {
        // SNS Topic ARN
        String snsTopicArn = "arn:aws-cn:sns:cn-north-1:175826060701:wecom_api_connection_alert_notification";

        // 创建 SNS 客户端
        SnsClient snsClient = SnsClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.builder().build()) // 使用 IAM Role
            .region(Region.CN_NORTH_1) // 设置区域
            .build();

        try {
            // 发布消息到 SNS Topic
            PublishRequest request = PublishRequest.builder()
                .topicArn(snsTopicArn)
                .subject("任务报警: GetChatData 持续失败")
                .message(message)
                .build();

            PublishResponse result = snsClient.publish(request);
            logger.info("SNS 报警消息已发送，MessageId: " + result.messageId());
        } catch (SnsException e) {
            logger.severe("发送 SNS 报警消息失败: " + e.getMessage());
        } finally {
            snsClient.close(); // 关闭 SNS 客户端
        }
    }

    private static void saveFailedRecord(String taskId, String errorType, int retCode) {
        String failedRecordsPath = curatedFilePath.replace("chat_", "failed_records_");
        try (CSVWriter writer = new CSVWriter(new FileWriter(failedRecordsPath, true))) {
            writer.writeNext(new String[]{taskId, errorType, String.valueOf(retCode)});
        } catch (Exception e) {
            logger.severe("保存失败记录失败: " + e.getMessage());
        }
    }
    private static void appendToRawFile(String content) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(rawFilePath, true))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            logger.severe("追加数据到 raw 文件失败: " + e.getMessage());
        }
    }
}
