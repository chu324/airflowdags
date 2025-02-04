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
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.zip.ZipEntry; 
import java.util.zip.ZipOutputStream; 
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;

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

    // 定时任务
    private static ScheduledExecutorService scheduler;

    // 声明 logAggregator 为静态变量
    private static final LogAggregator logAggregator = new LogAggregator();

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
        rawFilePath = rawDirPath + "/wecom_chat_" + taskDateStr + "_raw.csv";

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
            logger.warning("解密并保存到 curated 文件失败，但仍将继续下载媒体文件");
        }

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
    
                // **新增：每 60 秒打印一次日志**
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
        int totalCount = 0; // 总数据条数
        int successCount = 0; // 解密成功的条数
        int failureCount = 0; // 解密失败的条数
        List<String> failureDetails = new ArrayList<>(); // 存储解密失败的详细信息

        // 定义 media_files.csv 文件路径
        String mediaFilesPath = curatedFilePath.replace("chat_", "media_files_");

        // 使用 Set 去重
        Set<String> processedSdkFileIds = new HashSet<>();

        try (BufferedReader rawReader = new BufferedReader(new FileReader(rawFilePath));
             BufferedWriter curatedWriter = new BufferedWriter(new FileWriter(curatedFilePath, true));
             BufferedWriter mediaWriter = new BufferedWriter(new FileWriter(mediaFilesPath, true))) {

            // 如果是首次写入，添加表头
            if (new File(curatedFilePath).length() == 0) {
                String header = "seq,msgid,action,sender,receiver,roomid,msgtime,msgtype,sdkfileid,raw_json";
                curatedWriter.write(header);
                curatedWriter.newLine();
            }

            // 如果是首次写入 media_files.csv，添加表头
            if (new File(mediaFilesPath).length() == 0) {
                String mediaHeader = "msgtype,sdkfileid";
                mediaWriter.write(mediaHeader);
                mediaWriter.newLine();
            }

            String line;
            while ((line = rawReader.readLine()) != null) {
                totalCount++; // 统计总数据条数

                JsonNode rootNode = objectMapper.readTree(line);
                JsonNode chatDataNode = rootNode.path("chatdata");

                if (chatDataNode.isMissingNode() || !chatDataNode.isArray()) {
                    logger.severe("chatdata 字段缺失或格式错误");
                    failureCount++; // 统计解密失败的条数
                    continue;
                }

                for (JsonNode chatData : chatDataNode) {
                    String seqStr = chatData.path("seq").asText();
                    String msgid = chatData.path("msgid").asText();
                    String encryptRandomKey = chatData.path("encrypt_random_key").asText();
                    String encryptChatMsg = chatData.path("encrypt_chat_msg").asText();

                    // 解密数据
                    String decryptedMsg = decryptData(sdk, encryptRandomKey, encryptChatMsg);
                    if (decryptedMsg == null) {
                        failureCount++; // 统计解密失败的条数
                        failureDetails.add("seq=" + seqStr + ", msgid=" + msgid); // 记录解密失败的详细信息
                        continue;
                    }

                    // 转义控制字符
                    String escapedDecryptedMsg = escapeControlCharacters(decryptedMsg);

                    successCount++; // 统计解密成功的条数

                    // 解析解密后的消息
                    JsonNode decryptedRootNode = objectMapper.readTree(escapedDecryptedMsg);
                    String action = decryptedRootNode.path("action").asText();
                    String sender = decryptedRootNode.path("from").asText();
                    JsonNode tolistNode = decryptedRootNode.path("tolist");
                    String roomid = decryptedRootNode.path("roomid").asText();
                    long msgtime = decryptedRootNode.path("msgtime").asLong();
                    String msgtype = decryptedRootNode.path("msgtype").asText();

                    // 提取 sdkfileid
                    String sdkfileid = extractSeqFileId(decryptedRootNode, msgtype);
                    if (sdkfileid == null || sdkfileid.isEmpty()) {
                        sdkfileid = ""; // 如果 sdkfileid 为空，设置为空字符串
                    }

                    // 如果 msgtype 是媒体文件类型，并且 sdkfileid 未被处理过，写入 media_files.csv
                    Set<String> validMsgTypes = new HashSet<>(Arrays.asList(
                        "image", "voice", "video", "emotion", "file", "meeting_voice_call", "voip_doc_share"
                    ));
                    if (validMsgTypes.contains(msgtype) && !sdkfileid.isEmpty() && !processedSdkFileIds.contains(sdkfileid)) {
                        mediaWriter.write(msgtype + "," + sdkfileid);
                        mediaWriter.newLine();
                        processedSdkFileIds.add(sdkfileid); // 标记为已处理
                    }

                    // 提取 msgtype 对应的值作为 raw_json
                    JsonNode rawJsonNode = decryptedRootNode.path(msgtype);
                    String rawJson = rawJsonNode.toString();

                    // 将 UTC 时间戳转换为北京时间
                    Instant instant = Instant.ofEpochMilli(msgtime);
                    ZonedDateTime beijingTime = instant.atZone(ZoneId.of("Asia/Shanghai"));
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String beijingTimeStr = beijingTime.format(formatter);

                    // 对 rawJson 字段进行转义并用双引号包裹
                    String escapedRawJson = escapeCsvField(rawJson);

                    // 写入 curated 文件
                    if (tolistNode.isArray()) {
                        for (JsonNode to : tolistNode) {
                            String receiver = to.asText();
                            curatedWriter.write(seqStr + "," + msgid + "," + action + "," + sender + "," + receiver + "," + roomid + "," + beijingTimeStr + "," + msgtype + "," + sdkfileid + "," + escapedRawJson);
                            curatedWriter.newLine();
                        }
                    } else {
                        String receiver = tolistNode.toString();
                        curatedWriter.write(seqStr + "," + msgid + "," + action + "," + sender + "," + receiver + "," + roomid + "," + beijingTimeStr + "," + msgtype + "," + sdkfileid + "," + escapedRawJson);
                        curatedWriter.newLine();
                    }
                }
            }
        } catch (Exception e) {
            logger.severe("解密并保存到 curated 文件失败: " + e.getMessage());
            return false;
        }

        // 输出总结日志
        logger.info(String.format(
            "解密完成: 总数据条数=%d, 成功=%d, 失败=%d",
            totalCount, successCount, failureCount
        ));

        // 输出解密失败的详细信息
        if (!failureDetails.isEmpty()) {
            logger.info("解密失败的数据: " + String.join("; ", failureDetails));
        }

        logger.info("解密并保存到 curated 文件完成: " + curatedFilePath);
        logger.info("媒体文件信息已保存到: " + mediaFilesPath);
        return true;
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

    private static boolean downloadMediaFilesToS3(long sdk) {
        boolean allFilesDownloaded = true;
    
        // 定义 media_files.csv 文件路径
        String mediaFilesPath = curatedFilePath.replace("chat_", "media_files_");
    
        // 初始化日志聚合器
        LogAggregator logAggregator = new LogAggregator(); // 移除这一行，因为它已经在类级别声明了
    
        // 定时任务，每10分钟输出一次统计信息
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            logAggregator.logStatistics();
        }, 0, 10, TimeUnit.MINUTES);
    
        try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(mediaFilesPath))
                .withCSVParser(new CSVParserBuilder()
                        .withQuoteChar('"')
                        .withEscapeChar('\\')
                        .withIgnoreLeadingWhiteSpace(true)
                        .build())
                .build()) {
            String[] fields;
            boolean isHeader = true;
    
            // 创建线程池，并行度为 100
            ExecutorService executorService = Executors.newFixedThreadPool(20);
    
            while ((fields = csvReader.readNext()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
    
                if (fields.length < 2) {
                    logger.severe("CSV 行格式错误，字段不足: " + Arrays.toString(fields));
                    continue;
                }
    
                String msgtype = fields[0];
                String sdkfileid = fields[1];
    
                // 更新总文件数统计
                logAggregator.incrementTotalMediaFiles(); // 使用类级别的 logAggregator
    
                // 为每个任务创建独立的 SDK 实例
                executorService.submit(() -> {
                    long taskSdk = Finance.NewSdk();
                    Finance.Init(taskSdk, "wx1b5619d5190a04e4", "qY6ukRvf83VOi6ZTqVIaKiz93_iDbDGqVLBaSKXJCBs");
                    try {
                        boolean fileDownloaded = downloadAndUploadMediaFile(taskSdk, sdkfileid, msgtype);
                        if (fileDownloaded) {
                            logAggregator.incrementSuccessCount(); // 使用类级别的 logAggregator
                        } else {
                            logAggregator.incrementFailureCount(); // 使用类级别的 logAggregator
                            logAggregator.logFailureCategory("下载失败", sdkfileid); // 使用类级别的 logAggregator
                        }
                    } catch (Exception e) {
                        logAggregator.incrementFailureCount(); // 使用类级别的 logAggregator
                        logAggregator.logFailureCategory("异常", sdkfileid); // 使用类级别的 logAggregator
                    } finally {
                        Finance.DestroySdk(taskSdk);
                    }
                });
            }
    
            // 关闭线程池并等待所有任务完成
            executorService.shutdown();
            // 关闭线程池并等待所有任务完成
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.severe("线程池关闭中断: " + e.getMessage());
            }
    
            // 关闭定时任务
            scheduler.shutdown();
    
            // 最终统计信息
            logAggregator.logStatistics(); // 使用类级别的 logAggregator
    
            logger.info("所有媒体文件已处理完成");
            return allFilesDownloaded;
    
        } catch (IOException | CsvValidationException e) {
            logger.severe("读取 media_files.csv 文件失败: " + e.getMessage());
            return false;
        }
    }

    private static boolean downloadAndUploadMediaFile(long sdk, String sdkfileid, String msgtype) {
        if (sdkfileid == null || sdkfileid.isEmpty()) {
            logger.info("sdkfileid 为空，跳过下载: msgtype=" + msgtype);
            return false;
        }
    
        String indexbuf = "";
        boolean isFinished = false;
    
        final long RETRY_INTERVAL = 10 * 1000;
        final long MAX_RETRY_TIME = 30 * 60 * 1000;
        long retryStartTime = System.currentTimeMillis();
        boolean isInRetryPeriod = false;
    
        File tempFile = null;
        try {
            tempFile = File.createTempFile("media_", ".tmp");
            try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                long retryCount = 0;
                while (!isFinished) {
                    long mediaData = Finance.NewMediaData();
    
                    int ret = Finance.GetMediaData(sdk, indexbuf, sdkfileid, null, null, 10, mediaData);
    
                    if (ret != 0) {
                        if (ret == 10010) {
                            Finance.FreeMediaData(mediaData);
                            return false;
                        } else if (ret == 10001) {
                            retryCount++;
                            if (!isInRetryPeriod) {
                                isInRetryPeriod = true;
                                retryStartTime = System.currentTimeMillis();
                            }
    
                            long currentTime = System.currentTimeMillis();
                            long elapsedTime = currentTime - retryStartTime;
    
                            if (elapsedTime < MAX_RETRY_TIME) {
                                logger.warning("SDK 返回 10001，正在重试，重试次数: " + retryCount);
                                Thread.sleep(RETRY_INTERVAL);
                                continue;
                            } else {
                                logger.severe("GetMediaData failed, ret: " + ret + ". 当前重试超过最大重试时间.");
                                Finance.FreeMediaData(mediaData);
                                logAggregator.logFailureCategory("下载超时", sdkfileid);
                                saveFailedRecordsToCSV("下载超时", sdkfileid, ret);
                                return false;
                            }
                        } else {
                            logger.severe("GetMediaData failed, ret: " + ret + ". 任务中断。");
                            logAggregator.logApiErrorCode(ret);
                            Finance.FreeMediaData(mediaData);
                            logAggregator.logFailureCategory("API 错误（非 10001）", sdkfileid);
                            saveFailedRecordsToCSV("API 错误（非 10001）", sdkfileid, ret);
                            return false;
                        }
                    }
    
                    if (isInRetryPeriod) {
                        isInRetryPeriod = false;
                        retryStartTime = 0;
                    }
    
                    fileOutputStream.write(Finance.GetData(mediaData));
                    isFinished = Finance.IsMediaDataFinish(mediaData) == 1;
                    if (!isFinished) {
                        indexbuf = Finance.GetOutIndexBuf(mediaData);
                    }
                    Finance.FreeMediaData(mediaData);
                }
            }
    
            // 生成 S3 文件路径
            String s3Key;
            if ("unknown".equals(msgtype)) {
                s3Key = String.format("raw/wecom_chat/media/unknown/%s.%s",
                        sdkfileid, getFileExtension(getOriginalFileName(sdkfileid, msgtype)));
            } else {
                s3Key = String.format("raw/wecom_chat/media/%s/%s.%s",
                        msgtype, sdkfileid, getFileExtension(getOriginalFileName(sdkfileid, msgtype)));
            }
    
            // 上传临时文件到 S3
            uploadFileToS3(tempFile.getAbsolutePath(), mediaS3BucketName, s3Key);
            return true;
    
        } catch (IOException e) {
            logger.severe("文件操作失败: " + e.getMessage());
            logAggregator.logFailureCategory("文件操作失败", sdkfileid);
            saveFailedRecordsToCSV("文件操作失败", sdkfileid, -1);
            return false;
        } catch (Exception e) {
            logger.severe("未知异常: " + e.getMessage());
            logAggregator.logFailureCategory("未知异常", sdkfileid);
            saveFailedRecordsToCSV("未知异常", sdkfileid, -1);
            return false;
        } finally {
            if (tempFile != null && tempFile.exists()) {
                if (!tempFile.delete()) {
                    logger.warning("临时文件删除失败: " + tempFile.getAbsolutePath());
                }
            }
        }
    }

    private static String getFileExtension(String fileName) {
        int lastDotIndex = fileName.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return ""; // 没有扩展名
        }
        return fileName.substring(lastDotIndex + 1);
    }

    private static String getOriginalFileName(String sdkfileid, String msgtype) {
        switch (msgtype) {
            case "image":
                return sdkfileid + ".jpg";
            case "voice":
                return sdkfileid + ".mp3";
            case "video":
                return sdkfileid + ".mp4";
            case "emotion":
                return sdkfileid + ".jpg";
            case "file":
                try {
                    if (isJson(sdkfileid)) {
                        JsonNode fileNode = objectMapper.readTree(sdkfileid);
                        String filename = fileNode.path("filename").asText();
                        String fileext = fileNode.path("fileext").asText();
                        return filename + "." + fileext;
                    }
                    return sdkfileid + ".bin";
                } catch (Exception e) {
                    logger.severe("解析文件信息失败: " + e.getMessage());
                    return sdkfileid + ".bin";
                }
            default:
                return sdkfileid + ".bin";
        }
    }
    
    // 判断字符串是否为有效的 JSON
    private static boolean isJson(String content) {
        try {
            objectMapper.readTree(content);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean compressAndUploadFilesToS3() {
        try {
            // 压缩并上传 raw 文件
            String rawZipFilePath = compressFile(rawFilePath, "wecom_chat_" + taskDateStr + "_raw.zip");
            String rawS3Key = "home/wecom/inbound/c360/chat/archival/" + taskDateStr + "/wecom_chat_" + taskDateStr + "_raw.zip";
            logger.info("开始上传 raw 文件到 S3: " + rawS3Key);
            uploadFileToS3(rawZipFilePath, s3BucketName, rawS3Key);
            logger.info("raw 文件上传完成: " + rawS3Key);
    
            // 压缩并上传 curated 文件
            String curatedZipFilePath = compressFile(curatedFilePath, "chat_" + taskDateStr + ".zip");
            String curatedS3Key = "home/wecom/inbound/c360/chat/" + taskDateStr + "/chat_" + taskDateStr + ".zip";
            logger.info("开始上传 curated 文件到 S3: " + curatedS3Key);
            uploadFileToS3(curatedZipFilePath, s3BucketName, curatedS3Key);
            logger.info("curated 文件上传完成: " + curatedS3Key);
    
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

    /**
     * 删除目录下的所有内容
     */
    private static void deleteDirectoryContents(String directoryPath) {
        try {
            // 检查目录是否存在
            File directory = new File(directoryPath);
            if (!directory.exists()) {
                logger.info("目录不存在，无需清理: " + directoryPath);
                return;
            }
    
            // 删除目录下的所有文件和子目录
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        // 递归删除子目录
                        deleteDirectoryContents(file.getAbsolutePath());
                        // 删除子目录本身
                        if (file.delete()) {
                            logger.info("成功删除子目录: " + file.getAbsolutePath());
                        } else {
                            logger.severe("删除子目录失败: " + file.getAbsolutePath());
                        }
                    } else {
                        // 删除文件
                        if (file.delete()) {
                            logger.info("成功删除文件: " + file.getAbsolutePath());
                        } else {
                            logger.severe("删除文件失败: " + file.getAbsolutePath());
                        }
                    }
                }
            } else {
                logger.info("目录为空，无需清理: " + directoryPath);
            }
        } catch (Exception e) {
            logger.severe("清理目录时发生异常: " + e.getMessage());
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

    private static void saveFailedRecordsToCSV(String errorType, String sdkfileid, int retCode) {
        String failedRecordsFilePath = curatedFilePath.replace("chat_", "failed_records_");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(failedRecordsFilePath, true))) {
            // 格式：错误类型（ret代码）,sdkfileid
            writer.write(errorType + " (" + retCode + ")," + sdkfileid);
            writer.newLine();
            logAggregator.incrementFailedRecordsCount();
        } catch (IOException e) {
            logger.severe("记录失败文件信息失败: " + e.getMessage());
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
