package com.tencent.wework;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Map;

public class LogAggregator {
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

    public void logStatistics() {
        System.out.println("定时任务触发，正在输出统计信息...");
        System.out.println(String.format("[媒体文件下载统计] 总文件数=%d, 成功=%d, 失败=%d, 过期=%d",
                totalMediaFiles.get(), successCount.get(), failureCount.get(), expiredCount.get()));

        System.out.println(String.format("失败文件总数=%d", failedRecordsCount.get()));

        if (!failureCategories.isEmpty()) {
            System.out.println("失败分类统计:");
            for (Map.Entry<String, AtomicInteger> entry : failureCategories.entrySet()) {
                System.out.println("- " + entry.getKey() + ": " + entry.getValue().get());
            }
        }

        // 输出 API 错误码统计（排除 10001）
        if (!apiErrorCodesMap.isEmpty()) {
            System.out.println("API 错误码统计（非 10001）:");
            for (Map.Entry<Integer, AtomicInteger> entry : apiErrorCodesMap.entrySet()) {
                System.out.println("- ret=" + entry.getKey() + ": " + entry.getValue().get());
            }
        }
    }
}
