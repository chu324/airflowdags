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
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    public static String taskDateStr = null;
    
    // S3 bucket configuration
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
            .apiCallTimeout(Duration.ofMinutes(5))
        )
        .httpClient(UrlConnectionHttpClient.create())
        .build();

    private static String rawFilePath = null;
    private static String curatedFilePath = null;
    private static int totalTasks;
    private static ScheduledExecutorService scheduler;
    private static String accessToken;
    private static long tokenExpiresTime;
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILED = "failed";
    private static final AtomicInteger apiCallCount = new AtomicInteger(0);
    private static final AtomicInteger successfulUnionIdCount = new AtomicInteger(0);
    private static final AtomicInteger noUnionIdCount = new AtomicInteger(0);
    private static final ExecutorService networkRetryExecutor = Executors.newFixedThreadPool(10);
    
    // Constants
    private static final long MAX_RETRY_DURATION = Duration.ofMinutes(30).toMillis();
    private static final long MAX_SLEEP = Duration.ofSeconds(30).toMillis();
    private static final long STATUS_INTERVAL = Duration.ofMinutes(1).toMillis();
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_SIZE = 1000;

    public static class SdkException extends RuntimeException {
        private final int statusCode;
    
        public SdkException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }
    
        public SdkException(int statusCode, String message, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }
    
        public int getStatusCode() {
            return statusCode;
        }
    }

    /**
     * 获取企业微信的access_token
     */
    private static String getAccessToken(String corpid, String corpsecret) {
        if (accessToken != null && tokenExpiresTime > System.currentTimeMillis()) {
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
                    tokenExpiresTime = System.currentTimeMillis() + rootNode.path("expires_in").asLong() * 1000 - 60 * 1000;
                    return accessToken;
                } else {
                    logger.severe("获取access_token失败，错误码：" + rootNode.path("errcode").asInt() + 
                                "，错误信息：" + rootNode.path("errmsg").asText());
                }
            } else {
                logger.severe("HTTP请求失败，状态码：" + response.statusCode());
            }
        } catch (Exception e) {
            logger.severe("获取access_token异常：" + e.getMessage());
        }
    
        return null;
    }

    /**
     * 通过external_userid获取unionid
     */
    private static String getUnionIdByExternalUserId(String externalUserId) {
        if (StringUtils.isBlank(externalUserId)) {
            logger.warning("externalUserId为空");
            return null;
        }

        String corpid = KeyConfigFromCsv.getCorpId();
        String corpsecret = KeyConfigFromCsv.getUnionidSecret();
    
        String accessToken = getAccessToken(corpid, corpsecret);
        if (accessToken == null || accessToken.isEmpty()) {
            logger.severe("获取access_token失败，无法调用API");
            return null;
        }
    
        String apiUrl = String.format(
            "https://qyapi.weixin.qq.com/cgi-bin/externalcontact/get?access_token=%s&external_userid=%s",
            URLEncoder.encode(accessToken, StandardCharsets.UTF_8),
            URLEncoder.encode(externalUserId, StandardCharsets.UTF_8)
        );
    
        long startTime = System.currentTimeMillis();
        int attempt = 0;
    
        while (true) {
            try {
                HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(15))
                    .build();
    
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .timeout(Duration.ofSeconds(20))
                    .build();
    
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    
                int currentApiCall = apiCallCount.incrementAndGet();
                if (currentApiCall % 1000 == 0) {
                    logger.info(String.format(
                        "API调用统计: 成功返回unionid=%d, 无对应unionid=%d",
                        successfulUnionIdCount.get(),
                        noUnionIdCount.get()
                    ));
                }
    
                if (response.statusCode() == 200) {
                    JsonNode rootNode = objectMapper.readTree(response.body());
                    int errCode = rootNode.path("errcode").asInt(-1);
                    String errMsg = rootNode.path("errmsg").asText("未知错误");
    
                    if (errCode == 0) {
                        if (rootNode.has("external_contact") && 
                            rootNode.get("external_contact").has("unionid")) {
                            String unionid = rootNode.get("external_contact").get("unionid").asText();
                            successfulUnionIdCount.incrementAndGet();
                            return unionid;
                        } else {
                            //logger.warning("响应中缺少unionid字段: " + rootNode);
                            return null;
                        }
                    } else if (errCode == 84061) {
                        noUnionIdCount.incrementAndGet();
                        //logger.warning(String.format(
                        //    "external_userid=%s无对应unionid，错误码：%d，错误信息：%s",
                        //    externalUserId, errCode, errMsg
                        //));
                        return null;
                    } else {
                        logger.warning(String.format(
                            "API调用失败，错误码：%d，错误信息：%s",
                            errCode, errMsg
                        ));
                    }
                } else {
                    logger.severe("HTTP请求失败，状态码：" + response.statusCode());
                }
            } catch (IOException | InterruptedException e) {
                //logger.severe("获取unionid失败：" + e.getMessage());
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
    
            attempt++;
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > MAX_RETRY_DURATION) {
                logger.severe(String.format(
                    "重试时间已用尽，external_userid=%s，累计重试次数=%d",
                    externalUserId, attempt
                ));
                return null;
            }
    
            long baseDelay = (long) Math.pow(2, attempt) * 1000;
            long jitter = ThreadLocalRandom.current().nextLong(500, 1500);
            long sleepTime = Math.min(baseDelay + jitter, MAX_SLEEP);
    
            logger.warning(String.format(
                "调用API失败，external_userid=%s，第%d次重试，休眠%.1f秒",
                externalUserId, attempt, sleepTime / 1000.0
            ));
    
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.severe("线程被中断: " + ie.getMessage());
                return null;
            }
        }
    }

    /**
     * 生成并上传外部用户ID列表
     */
    private static void generateAndUploadExternalUserIds(String chatFilePath, String curatedDirPath) {
        if (StringUtils.isBlank(chatFilePath) || StringUtils.isBlank(curatedDirPath)) {
            logger.severe("参数chatFilePath或curatedDirPath为空");
            return;
        }

        Set<String> externalUserIds = ConcurrentHashMap.newKeySet();
        String tmpCsvPath = curatedDirPath + "/wecom_external_userid.csv";
        
        try {
            // 确保目录存在
            Files.createDirectories(Paths.get(curatedDirPath));
            
            // 1. 读取chat文件提取sender和receiver
            try (CSVReader csvReader = new CSVReader(new FileReader(chatFilePath))) {
                csvReader.readNext(); // 跳过表头
                String[] record;
                while ((record = csvReader.readNext()) != null) {
                    if (record.length >= 5) {
                        String sender = record[3];
                        String receiver = record[4];
                        if (isExternalUser(sender)) externalUserIds.add(sender);
                        if (isExternalUser(receiver)) externalUserIds.add(receiver);
                    }
                }
            }
    
            // 2. 写入CSV文件
            try (CSVWriter csvWriter = new CSVWriter(new FileWriter(tmpCsvPath))) {
                csvWriter.writeNext(new String[]{"external_userid"});
                externalUserIds.forEach(id -> csvWriter.writeNext(new String[]{id}));
            }
    
            // 3. 上传到S3
            String s3Key = "stage/tmp/wecom_external_userid.csv";
            uploadFileToS3(tmpCsvPath, mediaS3BucketName, s3Key);
            logger.info("外部用户ID列表已上传至S3: " + s3Key);
    
        } catch (Exception e) {
            logger.severe("处理外部用户ID失败: " + e.getMessage());
        }
    }

    /**
     * 加载Redshift连接信息
     */
    private static Map<String, String> loadRedshiftSecret() {
        String secretName = "prd".equalsIgnoreCase(System.getenv("env")) 
            ? "prd/datafabric_service/redshift" 
            : "qa/datafabric_service/redshift";
        
        try (SecretsManagerClient client = SecretsManagerClient.create()) {
            GetSecretValueResponse response = client.getSecretValue(
                GetSecretValueRequest.builder().secretId(secretName).build()
            );
            JsonNode secretJson = objectMapper.readTree(response.secretString());
            
            Map<String, String> credentials = new HashMap<>();
            credentials.put("username", secretJson.path("username").asText());
            credentials.put("password", secretJson.path("password").asText());
            credentials.put("host", secretJson.path("host").asText());
            credentials.put("port", secretJson.path("port").asText());
            return credentials;
        } catch (Exception e) {
            logger.severe("获取Redshift凭据失败: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 构造Redshift JDBC URL
     */
    private static String getRedshiftJdbcUrl() {
        Map<String, String> creds = loadRedshiftSecret();
        String dbName = "prd".equalsIgnoreCase(System.getenv("env")) ? "prd_c360" : "qa_c360";
        return String.format("jdbc:redshift://%s:%s/%s?user=%s&password=%s",
            creds.get("host"), creds.get("port"), dbName,
            creds.get("username"), creds.get("password"));
    }

    /**
     * 执行COPY命令
     */
    private static void executeCopyCommand() {
        String env = System.getenv("env");
        String iamRole = "prd".equalsIgnoreCase(env) 
            ? "arn:aws-cn:iam::175814205108:role/datalab-cn-redshift-prd-cn-north-1-role"
            : "arn:aws-cn:iam::175826060701:role/datalab-cn-redshift-qa-cn-north-1-role";
        
        String s3Path = String.format("s3://%s/stage/tmp/wecom_external_userid.csv", 
            "prd".equalsIgnoreCase(env) ? "175814205108-eds-prd-cn-north-1" : "175826060701-eds-qa-cn-north-1");
    
        String copySql = String.format(
            "TRUNCATE TABLE stage.wecom_external_userid; " +
            "COPY stage.wecom_external_userid " +
            "FROM '%s' " +
            "IAM_ROLE '%s' " +
            "DELIMITER ',' " +
            "FORMAT CSV " +
            "ENCODING UTF8 " +
            "IGNOREHEADER 1;", 
            s3Path, iamRole
        );
    
        try (Connection conn = DriverManager.getConnection(getRedshiftJdbcUrl());
             Statement stmt = conn.createStatement()) {
            stmt.execute(copySql);
            logger.info("COPY命令执行成功");
        } catch (SQLException e) {
            logger.severe("COPY命令执行失败: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取需处理的external_userid列表
     */
    private static List<String> getNewExternalUserIds() {
        String sql = "SELECT s.external_userid " +
                     "FROM stage.wecom_external_userid s " +
                     "LEFT JOIN landing.wecom_unionid l " +
                     "ON s.external_userid = l.external_userid " +
                     "WHERE l.external_userid IS NULL;";
    
        List<String> newUserIds = new ArrayList<>();
        int redshiftCount = 0;
        try (Connection conn = DriverManager.getConnection(getRedshiftJdbcUrl());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                newUserIds.add(rs.getString("external_userid"));
                redshiftCount++;
            }
        } catch (SQLException e) {
            logger.severe("查询新增用户失败: " + e.getMessage());
            throw new RuntimeException(e);
        }

        // 获取上传的源文件的行数
        String sourceFilePath = "data/curated/" + taskDateStr + "/wecom_external_userid.csv";
        long sourceFileCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(sourceFilePath))) {
            sourceFileCount = reader.lines().skip(1).count(); // 跳过表头
        } catch (IOException e) {
            logger.severe("读取源文件失败: " + e.getMessage());
        }

        // 打印日志对比 Redshift 返回的数量和源文件的数量
        logger.info(String.format(
            "Redshift返回的external_userid数量: %d, 上传的源文件external_userid数量: %d",
            redshiftCount, sourceFileCount
        ));

        return newUserIds;
    }

    /**
     * 生成最终的UnionID映射文件
     */
    private static void generatUnionIdMappingCsv(Map<String, String> unionIdMap) {
        if (unionIdMap == null || unionIdMap.isEmpty()) {
            logger.warning("UnionID映射为空，跳过文件生成");
            return;
        }
    
        String mappingFileName = "unionid_" + taskDateStr + ".csv";
        String tmpCsvPath = curatedFilePath.replace("chat_", "unionid_");
    
        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(tmpCsvPath))) {
            csvWriter.writeNext(new String[]{"external_userid", "unionid"});
            unionIdMap.forEach((userId, unionId) -> 
                csvWriter.writeNext(new String[]{userId, unionId})
            );
            
            int rowCount = unionIdMap.size();
            logger.info("获取unionid总数: " + rowCount);
        } catch (IOException e) {
            logger.severe("写入UnionID映射文件失败: " + e.getMessage());
        }
    }

    /**
     * 判断是否为外部用户
     */
    private static boolean isExternalUser(String userId) {
        return userId != null && (userId.startsWith("wo") || userId.startsWith("wm")) && !userId.startsWith("wb");
    }

    /**
     * 主流程：获取新数据
     */
    public static boolean fetchNewData(long sdk) {
        // 设置任务日期
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Shanghai"));
        taskDateStr = now.format(formatter);
    
        // 初始化文件路径
        String rawDirPath = "data/raw/" + taskDateStr;
        String curatedDirPath = "data/curated/" + taskDateStr;
        
        try {
            Files.createDirectories(Paths.get(rawDirPath));
            Files.createDirectories(Paths.get(curatedDirPath));
        } catch (IOException e) {
            logger.severe("创建目录失败: " + e.getMessage());
            return false;
        }
        
        rawFilePath = rawDirPath + "/wecom_chat_" + taskDateStr + ".csv";
        curatedFilePath = curatedDirPath + "/chat_" + taskDateStr + ".csv";
    
        // 阶段1: 拉取数据
        if (!fetchData(sdk)) {
            return false;
        }
    
        // 阶段2: 解密数据
        if (!decryptAndSaveToCurated(sdk)) {
            logger.warning("解密并保存到curated文件失败");
            return false;
        }

        // 阶段3: 提取并上传外部用户ID到S3
        generateAndUploadExternalUserIds(curatedFilePath, curatedDirPath);

        // 阶段4: 导入Redshift并过滤新增ID
        executeCopyCommand();
        List<String> externalUserIdsToProcess = getNewExternalUserIds();
        if (externalUserIdsToProcess.isEmpty()) {
            logger.info("无新增external_userid，跳过API调用");
            return true;
        }

        // 阶段5: 处理新增的external_userid
        Map<String, String> unionIdMap = new ConcurrentHashMap<>();
        externalUserIdsToProcess.parallelStream().forEach(userId -> {
            String unionId = getUnionIdByExternalUserId(userId);
            if (StringUtils.isNotBlank(unionId)) {
                unionIdMap.put(userId, unionId);
            } else {
                //logger.warning("未获取到unionid的external_userid: " + userId);
            }
        });

        // 阶段6: 生成UnionID映射文件
        generatUnionIdMappingCsv(unionIdMap);
        
        // 阶段7: 下载媒体文件到S3
        if (!downloadMediaFilesToS3(sdk)) {
            logger.severe("媒体文件下载失败率超过阈值");
            return false;
        }
        
        // 阶段8: 压缩并上传文件到S3
        if (!compressAndUploadFilesToS3()) {
            return false;
        }
    
        return true;
    }
    
    /**
     * 拉取聊天数据
     */
    private static boolean fetchData(long sdk) {
        int limit = BATCH_SIZE;
        int lastSeq = SeqHistoryUtil.loadLastSeq();
        boolean hasMoreData = true;
        final long RETRY_INTERVAL = Duration.ofSeconds(10).toMillis();
        boolean isInRetryPeriod = false;
        long retryStartTime = 0;
        int totalFetched = 0;
        long lastLogTime = System.currentTimeMillis();
    
        while (hasMoreData) {
            long slice = Finance.NewSlice();
            long batchStartTime = System.currentTimeMillis();
            try {
                long ret = Finance.GetChatData(sdk, lastSeq, limit, "", "", 10, slice);
    
                if (ret != 0) {
                    if (ret == 10001 || ret == 10002) {
                        if (!isInRetryPeriod) {
                            isInRetryPeriod = true;
                            retryStartTime = System.currentTimeMillis();
                            logger.info("检测到ret " + ret + "，开始记录波动时间，retryStartTime: " + retryStartTime);
                        }
                        long elapsedTime = System.currentTimeMillis() - retryStartTime;
                        if (elapsedTime < MAX_RETRY_DURATION) {
                            Thread.sleep(RETRY_INTERVAL);
                            continue;
                        } else {
                            logger.severe("GetChatData failed, ret: " + ret + ". 当前波动超过最大重试时间.");
                            return false;
                        }
                    } else {
                        logger.severe("GetChatData failed, ret: " + ret + ". 任务中断。");
                        return false;
                    }
                }
    
                if (isInRetryPeriod) {
                    isInRetryPeriod = false;
                    retryStartTime = 0;
                    logger.info("成功拉取数据，当前波动结束。");
                }
    
                String content = Finance.GetContentFromSlice(slice);
                if (StringUtils.isBlank(content)) {
                    logger.severe("GetContentFromSlice返回空数据");
                    return false;
                }
    
                appendToRawFile(content);
                JsonNode rootNode = objectMapper.readTree(content);
                JsonNode chatDataNode = rootNode.path("chatdata");
    
                if (chatDataNode.isMissingNode() || !chatDataNode.isArray()) {
                    logger.severe("chatdata字段缺失或格式错误");
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
    
                if (System.currentTimeMillis() - lastLogTime >= STATUS_INTERVAL) {
                    logger.info(String.format(
                        "[任务进度] 已拉取数据: %d条, 数据范围: seq=%d~%d, 耗时: %dms, 总拉取量: %d条",
                        batchSize, lastSeq - batchSize, lastSeq - 1, batchDuration, totalFetched
                    ));
                    lastLogTime = System.currentTimeMillis();
                }
    
                if (batchSize < limit) {
                    hasMoreData = false;
                    logger.info("所有数据已拉取完成！");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.severe("线程被中断: " + e.getMessage());
                return false;
            } catch (IOException e) {
                logger.severe("解析JSON失败: " + e.getMessage());
                return false;
            } catch (Exception e) {
                logger.severe("拉取数据失败: " + e.getMessage());
                if (!isInRetryPeriod) {
                    isInRetryPeriod = true;
                    retryStartTime = System.currentTimeMillis();
                }
                long elapsedTime = System.currentTimeMillis() - retryStartTime;
                if (elapsedTime < MAX_RETRY_DURATION) {
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

    /**
     * 追加数据到原始文件
     */
    private static synchronized void appendToRawFile(String content) {
        if (StringUtils.isBlank(content)) {
            logger.warning("尝试追加空内容到raw文件");
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(rawFilePath, true))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            logger.severe("写入raw文件失败: " + e.getMessage());
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 解密数据并保存到curated文件
     */
    private static boolean decryptAndSaveToCurated(long sdk) {
        int totalCount = 0;
        int successCount = 0;
        int failureCount = 0;
        List<String> failureDetails = new ArrayList<>();
    
        String tmpChatFilePath = curatedFilePath.replace("chat_", "tmp_chat_");
        String newChatFilePath = curatedFilePath;
    
        try (BufferedReader rawReader = new BufferedReader(new FileReader(rawFilePath));
             BufferedWriter tmpChatWriter = new BufferedWriter(new FileWriter(tmpChatFilePath, true));
             BufferedWriter newChatWriter = new BufferedWriter(new FileWriter(newChatFilePath, true))) {
    
            // 添加表头
            if (new File(tmpChatFilePath).length() == 0) {
                String tmpHeader = "seq,msgid,action,sender,receiver,roomid,msgtime,msgtype,sdkfileid,md5sum,fileext,raw_json";
                tmpChatWriter.write(tmpHeader);
                tmpChatWriter.newLine();
            }
    
            if (new File(newChatFilePath).length() == 0) {
                String newHeader = "seq,msgid,action,sender,receiver,roomid,msgtime,msgtype,sdkfileid,md5sum,raw_json";
                newChatWriter.write(newHeader);
                newChatWriter.newLine();
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
                    String fileext = extractFileExt(decryptedRootNode, msgtype);
    
                    JsonNode rawJsonNode = decryptedRootNode.path(msgtype);
                    String rawJson;
                    try {
                        objectMapper.readTree(rawJsonNode.toString());
                        rawJson = rawJsonNode.toString();

                        if ("chatrecord".equals(msgtype) && rawJson.length() > 60000) {
                            logger.warning(String.format("raw_json长度超过限制，seq=%s, msgid=%s, 原始长度=%d，已置为空", seqStr, msgid, rawJson.length()));
                            rawJson = "{}";
                        }
                    } catch (Exception e) {
                        logger.warning("无效的JSON内容，替换为默认值");
                        rawJson = "{}";
                    }
    
                    Instant instant = Instant.ofEpochMilli(msgtime);
                    ZonedDateTime beijingTime = instant.atZone(ZoneId.of("Asia/Shanghai"));
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String beijingTimeStr = beijingTime.format(formatter);
    
                    String escapedRawJson = escapeCsvField(rawJson);
    
                    // 写入tmp_chat文件
                    if (tolistNode.isArray()) {
                        for (JsonNode to : tolistNode) {
                            String receiver = to.asText();
                            tmpChatWriter.write(String.join(",",
                                seqStr, msgid, action, sender, receiver, roomid,
                                beijingTimeStr, msgtype, sdkfileid, md5sum, fileext, escapedRawJson
                            ));
                            tmpChatWriter.newLine();
                        }
                    } else {
                        String receiver = tolistNode.toString();
                        tmpChatWriter.write(String.join(",",
                            seqStr, msgid, action, sender, receiver, roomid,
                            beijingTimeStr, msgtype, sdkfileid, md5sum, fileext, escapedRawJson
                        ));
                        tmpChatWriter.newLine();
                    }
    
                    // 写入chat文件
                    if (tolistNode.isArray()) {
                        for (JsonNode to : tolistNode) {
                            String receiver = to.asText();
                            newChatWriter.write(String.join(",",
                                seqStr, msgid, action, sender, receiver, roomid,
                                beijingTimeStr, msgtype, sdkfileid, md5sum, escapedRawJson
                            ));
                            newChatWriter.newLine();
                        }
                    } else {
                        String receiver = tolistNode.toString();
                        newChatWriter.write(String.join(",",
                            seqStr, msgid, action, sender, receiver, roomid,
                            beijingTimeStr, msgtype, sdkfileid, md5sum, escapedRawJson
                        ));
                        newChatWriter.newLine();
                    }
                }
            }
    
            logger.info(String.format("解密完成: 总数据条数=%d, 成功=%d, 失败=%d", totalCount, successCount, failureCount));
            if (!failureDetails.isEmpty()) {
                logger.info("解密失败的数据: " + String.join("; ", failureDetails));
            }
            return true;
        } catch (Exception e) {
            logger.severe("解密并保存到curated文件失败: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 提取md5sum
     */
    private static String extractMd5Sum(JsonNode decryptedRootNode, String msgtype) {
        JsonNode msgTypeNode = decryptedRootNode.path(msgtype);
        return msgTypeNode.has("md5sum") ? msgTypeNode.path("md5sum").asText() : "";
    }

    /**
     * 提取文件扩展名
     */
    private static String extractFileExt(JsonNode decryptedRootNode, String msgtype) {
        if (!"file".equals(msgtype)) {
            return "";
        }
    
        JsonNode fileNode = decryptedRootNode.path("file");
        if (fileNode.isMissingNode()) {
            return "";
        }
    
        String fileext = fileNode.path("fileext").asText("");
        if (fileext.startsWith("{") && fileext.endsWith("}")) {
            try {
                JsonNode fileextNode = objectMapper.readTree(fileext);
                if (fileextNode.has("fileext")) {
                    return fileextNode.path("fileext").asText("");
                }
            } catch (Exception e) {
                logger.warning("fileext格式错误: " + fileext);
                return "";
            }
        }
        return fileext;
    }

    /**
     * 生成媒体文件清单JSON
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
                    if (record.length < 12) {
                        logger.warning(String.format("行 %d 字段不足（%d/12），跳过处理", currentLine, record.length));
                        skippedCount++;
                        continue;
                    }
    
                    String msgtype = record[7].trim();
                    String sdkfileid = record[8].trim();
                    String md5sum = record[9].trim();
                    String fileext = record[10].trim();
    
                    if (StringUtils.isAnyBlank(msgtype, sdkfileid, md5sum)) {
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
    
                    if (!"file".equals(msgtype)) {
                        fileext = getDefaultFileExt(msgtype);
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
                    taskJson.put("fileext", fileext);
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

    /**
     * 获取默认文件扩展名
     */
    private static String getDefaultFileExt(String msgtype) {
        switch (msgtype.toLowerCase()) {
            case "image": return "jpg";
            case "voice": return "mp3";
            case "video": return "mp4";
            case "emotion": return "gif";
            default: return "dat";
        }
    }

    /**
     * 校验消息类型是否有效
     */
    private static boolean isValidMsgTypeForMedia(String msgtype) {
        if (StringUtils.isBlank(msgtype)) return false;
        switch (msgtype.trim().toLowerCase()) {
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

    /**
     * 执行任务并重试
     */
    private static void executeTaskWithRetry(long sdk, JsonNode taskNode, AtomicInteger successCount, AtomicInteger failureCount) {
        String taskJson = taskNode.toString();
        String errorMsg = "";
        String msgtype = "";
        String sdkfileid = "";
        String fileext = "";
    
        try {
            msgtype = taskNode.path("msgtype").asText();
            sdkfileid = taskNode.path("sdkfileid").asText();
            String md5sum = taskNode.path("md5sum").asText();
            fileext = taskNode.path("fileext").asText();
    
            if (!isValidMsgTypeForMedia(msgtype) || !isValidSDKFileId(sdkfileid) || !isValidMD5(md5sum)) {
                errorMsg = "Invalid task parameters";
                throw new IllegalArgumentException(errorMsg);
            }
 
            File mediaFile = downloadMediaFile(sdk, sdkfileid, md5sum, msgtype, fileext);
            if (mediaFile == null) {
                errorMsg = "Download failed";
                throw new IOException(errorMsg);
            }
 
            String s3Key = getMediaS3Key(msgtype, md5sum, fileext);
            uploadFileToS3(mediaFile.getAbsolutePath(), mediaS3BucketName, s3Key);
            
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

    /**
     * 记录失败日志
     */
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

    /**
     * 校验SDK文件ID是否有效
     */
    private static boolean isValidSDKFileId(String sdkfileid) {
        return StringUtils.isNotBlank(sdkfileid);
    }

    /**
     * 解密数据
     */
    private static String decryptData(long sdk, String encryptRandomKey, String encryptChatMsg) {
        if (sdk <= 0 || StringUtils.isBlank(encryptRandomKey) || StringUtils.isBlank(encryptChatMsg)) {
            logger.severe("解密参数无效");
            return null;
        }

        try {
            byte[] encryptedKeyBytes = DecryptionUtil.base64Decode(encryptRandomKey);
            String privateKeyStr = KeyConfig.getPrivateKey();
            String decryptedKey = DecryptionUtil.rsaDecrypt(encryptedKeyBytes, privateKeyStr);

            long msgSlice = Finance.NewSlice();
            try {
                int ret = Finance.DecryptData(sdk, decryptedKey, encryptChatMsg, msgSlice);
                if (ret != 0) {
                    logger.severe("解密失败, ret: " + ret);
                    return null;
                }

                return Finance.GetContentFromSlice(msgSlice);
            } finally {
                Finance.FreeSlice(msgSlice);
            }
        } catch (Exception e) {
            logger.severe("解密失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 提取序列文件ID
     */
    private static String extractSeqFileId(JsonNode decryptedRootNode, String msgtype) {
        if (!isValidMsgTypeForMedia(msgtype)) return "";
    
        JsonNode msgTypeNode = decryptedRootNode.path(msgtype);
        String sdkfileid = msgTypeNode.path("sdkfileid").asText("").trim();
        
        if (sdkfileid.isEmpty() || sdkfileid.length() < 32) {
            logger.warning("Invalid sdkfileid detected: " + sdkfileid);
            return "";
        }
        return sdkfileid;
    }
    
    /**
     * 转义CSV字段
     */
    private static String escapeCsvField(String field) {
        if (field == null || field.isEmpty()) {
            return "\"\"";
        }
        
        StringWriter sw = new StringWriter();
        try (CSVWriter writer = new CSVWriter(sw)) {
            writer.writeNext(new String[]{field});
            String escaped = sw.toString().trim();
            
            if (escaped.endsWith("\n") || escaped.endsWith("\r")) {
                escaped = escaped.substring(0, escaped.length()-1);
            }
            return escaped;
        } catch (IOException e) {
            logger.warning("CSV字段转义失败: " + e.getMessage());
            return "\"" + field.replace("\"", "\"\"") + "\"";
        }
    }

    /**
     * 下载媒体文件
     */
    private static File downloadMediaFile(
        long sdk, 
        String sdkfileid, 
        String md5sum, 
        String msgtype,
        String fileext
    ) throws IOException, SdkException {
        if (sdk <= 0) {
            logger.severe("无效的SDK参数");
            throw new SdkException(-1, "无效的SDK参数");
        }

        final long taskStartTime = System.currentTimeMillis();
        int attempt = 0;
        File tempFile = null;
        String indexbuf = "";
    
        try {
            while (true) {
                try {
                    tempFile = createTempFile(md5sum, msgtype);
    
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
    
                        File finalFile = buildFinalFile(tempFile, md5sum, msgtype, fileext);
                        try {
                            Files.move(tempFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException e) {
                            logger.severe("移动文件失败: " + e.getMessage());
                            throw new SdkException(-1003, "移动文件失败: " + e.getMessage(), e);
                        }
    
                        long totalTime = System.currentTimeMillis() - taskStartTime;
                        double fileSizeMB = finalFile.length() / (1024.0 * 1024.0);
                        if (attempt > 0) {
                            logger.info(String.format(
                                "[媒体下载重试成功] 文件:%s 类型:%-6s 重试次数:%d 总耗时:%.1fs 文件大小:%.2fMB",
                                md5sum, msgtype, attempt, totalTime / 1000.0, fileSizeMB
                            ));
                        }
    
                        return finalFile;
                    }
                } catch (SdkException e) {
                    if (!isRetryableError(e.getStatusCode())) {
                        throw e;
                    }
    
                    long elapsed = System.currentTimeMillis() - taskStartTime;
                    if (elapsed > MAX_RETRY_DURATION) {
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
    
                    long baseDelay = (long) Math.pow(2, attempt) * 1000;
                    long jitter = ThreadLocalRandom.current().nextLong(500, 1500);
                    long sleepTime = Math.min(baseDelay + jitter, MAX_SLEEP);
    
                    logger.warning(String.format(
                        "[媒体下载重试] 文件:%s 类型:%-6s 第%02d次重试 错误码:%d 休眠:%.1fs 累计耗时:%.1fs",
                        md5sum, msgtype, attempt + 1,
                        e.getStatusCode(),
                        sleepTime / 1000.0,
                        elapsed / 1000.0
                    ));
    
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

    /**
     * 分类错误
     */
    private static String classifyError(int statusCode) {
        switch (statusCode) {
            case 10001: return "网络波动";
            case 10002: return "数据解析失败";
            case -1000: return "临时文件错误";
            case -1001: return "文件IO异常";
            default:    return "未知错误(" + statusCode + ")";
        }
    }

    /**
     * 创建文件输出流
     */
    private static FileOutputStream createFileOutputStream(File file) throws SdkException {
        try {
            return new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new SdkException(-1001, "文件未找到: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * 判断错误是否可重试
     */
    private static boolean isRetryableError(int statusCode) {
        return statusCode == 10001 || statusCode == 10002 || statusCode == 10003 || 
               statusCode == -1000 || statusCode == -1001;
    }

    /**
     * 创建临时文件
     */
    private static File createTempFile(String md5sum, String msgtype) throws IOException {
        String prefix = String.format("%s_%s_", md5sum, msgtype);
        File tempFile = Files.createTempFile(prefix, ".tmp").toFile();
        if (tempFile == null) {
            throw new IOException("创建临时文件失败");
        }
        return tempFile;
    }

    /**
     * 构建最终文件
     */
    private static File buildFinalFile(File tempFile, String md5sum, String msgtype, String fileext) {
        String ext = getFileExtension(msgtype, fileext);
        String fileName = md5sum + ext;
        return new File(tempFile.getParent(), fileName);
    }

    /**
     * 获取文件扩展名
     */
    private static String getFileExtension(String msgtype, String fileext) {
        switch (msgtype.toLowerCase()) {
            case "image": return ".jpg";
            case "voice": return ".mp3";
            case "video": return ".mp4";
            case "emotion": return ".gif";
            case "file":  return "." + fileext;
            default:      return ".dat";
        }
    }

    /**
     * 清理临时文件
     */
    private static void cleanTempFile(File file) {
        if (file != null && file.exists()) {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                logger.warning("清理临时文件失败: " + file.getAbsolutePath());
            }
        }
    }

    /**
     * 生成媒体文件名
     */
    private static String generateMediaFileName(String md5sum, String msgtype, String fileext) {
        String extension = getFileExtension(msgtype, fileext);
        return md5sum + extension;
    }

    /**
     * 获取媒体S3键
     */
    private static String getMediaS3Key(String msgtype, String md5sum, String fileext) {
        String safeMsgType = isValidMsgType(msgtype) ? msgtype : "unknown";
        return String.format("raw/wecom_chat/media/%s/%s",
            safeMsgType,
            generateMediaFileName(md5sum, msgtype, fileext)
        );
    }
            
    /**
     * 下载媒体文件到S3
     */
    private static boolean downloadMediaFilesToS3(long sdk) {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        String tmpChatFilePath = curatedFilePath.replace("chat_", "tmp_chat_");
        String mediaFilesPath = tmpChatFilePath.replace("chat_", "media_files_").replace(".csv", ".json");
        
        if (!new File(tmpChatFilePath).exists()) {
            logger.severe("tmpChatFilePath文件不存在: " + tmpChatFilePath);
            return false;
        }
        
        File mediaFilesDir = new File(mediaFilesPath).getParentFile();
        if (!mediaFilesDir.exists()) {
            mediaFilesDir.mkdirs();
        }
        
        new File("logs").mkdirs();
        
        totalTasks = generateMediaFilesJSON(tmpChatFilePath, mediaFilesPath);
        if (totalTasks < 0) {
            logger.severe("媒体清单生成失败");
            return false;
        }
        
        processMediaTasksStreaming(mediaFilesPath, sdk, successCount, failureCount);
        boolean finalStatus = waitForCompletion(totalTasks, successCount, failureCount);
        
        logProgress(totalTasks, successCount.get(), failureCount.get());
        
        return finalStatus;
    }

    /**
     * 流式处理媒体任务
     */
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
    
            List<JsonNode> batch = new ArrayList<>(BATCH_SIZE);
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                JsonNode taskNode = objectMapper.readTree(parser);
                batch.add(taskNode);
    
                if (batch.size() >= BATCH_SIZE) {
                    submitBatchTasks(batch, sdk, successCount, failureCount);
                    batch.clear();
                }
            }
    
            if (!batch.isEmpty()) {
                submitBatchTasks(batch, sdk, successCount, failureCount);
            }
    
        } catch (Exception e) {
            logger.severe("流式处理媒体任务失败: " + e.getMessage());
        }
    }
    
    /**
     * 提交批量任务
     */
    private static void submitBatchTasks(
        List<JsonNode> batch, 
        long sdk, 
        AtomicInteger successCount, 
        AtomicInteger failureCount
    ) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) networkRetryExecutor;
        for (JsonNode taskNode : batch) {
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

    /**
     * 等待任务完成
     */
    private static boolean waitForCompletion(int totalTasks, AtomicInteger successCount, AtomicInteger failureCount) {
        long lastStatusTime = System.currentTimeMillis();
    
        while (true) {
            int completed = successCount.get() + failureCount.get();
            long currentTime = System.currentTimeMillis();
    
            if (currentTime - lastStatusTime > STATUS_INTERVAL) {
                logProgress(completed, successCount.get(), failureCount.get());
                lastStatusTime = currentTime;
            }
    
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

    /**
     * 记录进度
     */
    private static void logProgress(int completed, int success, int failure) {
        double progress = (completed * 100.0) / totalTasks;
        String progressBar = buildProgressBar(progress);
        
        logger.info(String.format(
            "媒体下载进度: %s %d/%d (%.2f%%) | 成功=%d | 失败=%d",
            progressBar, completed, totalTasks, progress, success, failure
        ));
    }
    
    /**
     * 构建进度条
     */
    private static String buildProgressBar(double progress) {
        int bars = (int) (progress / 2);
        return String.format("[%-50s]", StringUtils.repeat("#", bars));
    }

    /**
     * 压缩并上传文件到S3
     */
    private static boolean compressAndUploadFilesToS3() {
        try {
            // 压缩并上传raw文件
            String rawZipFilePath = compressFile(rawFilePath, "wecom_chat_" + taskDateStr + ".zip");
            String rawS3Key = "home/wecom/inbound/c360/chat/archival/" + taskDateStr + "/wecom_chat_" + taskDateStr + ".zip";
            logger.info("开始上传raw文件到S3: " + rawS3Key);
            uploadFileToS3(rawZipFilePath, s3BucketName, rawS3Key);
            logger.info("raw文件上传完成: " + rawS3Key);
    
            // 压缩并上传curated文件
            String curatedZipFilePath = compressFile(curatedFilePath, "chat_" + taskDateStr + ".zip");
            String curatedS3Key = "home/wecom/inbound/c360/chat/" + taskDateStr + "/chat_" + taskDateStr + ".zip";
            logger.info("开始上传curated文件到S3: " + curatedS3Key);
            uploadFileToS3(curatedZipFilePath, s3BucketName, curatedS3Key);
            logger.info("curated文件上传完成: " + curatedS3Key);
    
            // 压缩并上传userid_mapping文件
            String mappingFilePath = curatedFilePath.replace("chat_", "unionid_");
            String mappingZipFilePath = compressFile(mappingFilePath, "unionid_" + taskDateStr + ".zip");
            String mappingS3Key = "home/wecom/inbound/c360/chat/" + taskDateStr + "/unionid_" + taskDateStr + ".zip";
            logger.info("开始上传unionid文件到S3: " + mappingS3Key);
            uploadFileToS3(mappingZipFilePath, s3BucketName, mappingS3Key);
            logger.info("unionid_文件上传完成: " + mappingS3Key);
    
            // 删除本地目录内容
            deleteDirectoryContents("data/raw");
            deleteDirectoryContents("data/curated");
    
            return true;
        } catch (Exception e) {
            logger.severe("压缩并上传文件到S3失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除目录内容
     */
    public static void deleteDirectoryContents(String dirPath) {
        deleteDirContents(new File(dirPath));
    }
    
    /**
     * 递归删除目录内容
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

    /**
     * 压缩文件
     */
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

        logger.info("文件已压缩为ZIP: " + zipFilePath);
        return zipFilePath;
    }

    /**
     * 上传文件到S3
     */
    private static void uploadFileToS3(String filePath, String s3BucketName, String s3Key) {
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

    /**
     * 计算指数退避时间
     */
    private static long calculateExponentialBackoff(int attempt) {
        long baseDelay = 1000;
        long maxDelay = 10000;
        long delay = (long) (baseDelay * Math.pow(2, attempt));
        return Math.min(delay, maxDelay) + new Random().nextInt(1000);
    }

    private static boolean isValidMD5(String md5sum) {
        return StringUtils.isNotBlank(md5sum);
    }

    private static boolean isValidMsgType(String msgtype) {
        return Set.of("image", "voice", "video", "emotion", "file").contains(msgtype);
    }

    /**
     * 转义控制字符
     */
    private static String escapeControlCharacters(String content) {
        return content.replaceAll("[\\p{Cntrl}]", " ");
    }
}
