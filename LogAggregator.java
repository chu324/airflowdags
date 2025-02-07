package com.tencent.wework;

import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.Level;

public class LogAggregator {
    private static final Logger logger = Logger.getLogger(LogAggregator.class.getName());

    private final AtomicInteger totalMediaFiles = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger expiredCount = new AtomicInteger(0);
    private final AtomicInteger failedRecordsCount = new AtomicInteger(0);

    // 分类统计失败情况
    private final Map<String, AtomicInteger> failureCategories = new HashMap<>();
    private final Map<String, List<String>> failureSdkFileIds = new HashMap<>();

    // 记录 API 返回的特定错误码
    private final Map<Integer, AtomicInteger> apiErrorCodesMap = new HashMap<>();

    public void incrementTotalMediaFiles() {
        totalMediaFiles.incrementAndGet();
    }

    public void incrementSuccessCount() {
        successCount.incrementAndGet();
    }

    public void incrementFailureCount() {
        failureCount.incrementAndGet();
    }

    public void incrementExpiredCount() {
        expiredCount.incrementAndGet();
    }

    public void incrementFailedRecordsCount() {
        failedRecordsCount.incrementAndGet();
    }

    public void logFailureCategory(String category, String sdkfileid) {
        failureCategories.computeIfAbsent(category, k -> new AtomicInteger(0)).incrementAndGet();
        failureSdkFileIds.computeIfAbsent(category, k -> new ArrayList<>()).add(sdkfileid);
    }

    public void logApiErrorCode(int retCode) {
        apiErrorCodesMap.computeIfAbsent(retCode, k -> new AtomicInteger(0)).incrementAndGet();
    }

    public void setTotalMediaFiles(int total) {
        this.totalMediaFiles.set(total);
    }

    public void logStatistics() {
        // 获取当前北京时间
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Shanghai"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestamp = now.format(formatter);

        // 输出统计信息
        logger.info(String.format(
            "[%s] 定时任务触发，正在输出统计信息...\n" +
            "[媒体文件下载统计] 总文件数=%d, 成功=%d, 失败=%d, 过期=%d\n" +
            "失败文件总数=%d",
            timestamp, 
            totalMediaFiles.get(), 
            successCount.get(), 
            failureCount.get(), 
            expiredCount.get(),
            failedRecordsCount.get()
        ));

        if (!failureCategories.isEmpty()) {
            logger.info(String.format("失败分类统计 (北京时间：%s):", timestamp));
            for (Map.Entry<String, AtomicInteger> entry : failureCategories.entrySet()) {
                logger.info(String.format("- %s: %d", entry.getKey(), entry.getValue().get()));
            }
        }

        // 输出 API 错误码统计（排除 10001）
        Map<Integer, AtomicInteger> filteredApiErrorCodes = new HashMap<>();
        for (Map.Entry<Integer, AtomicInteger> entry : apiErrorCodesMap.entrySet()) {
            if (entry.getKey() != 10001) {
                filteredApiErrorCodes.put(entry.getKey(), entry.getValue());
            }
        }

        if (!filteredApiErrorCodes.isEmpty()) {
            logger.info("API 错误码统计（非 10001）:");
            for (Map.Entry<Integer, AtomicInteger> entry : filteredApiErrorCodes.entrySet()) {
                logger.info(String.format("- ret=%d: %d", entry.getKey(), entry.getValue().get()));
            }
        }
    }
}
