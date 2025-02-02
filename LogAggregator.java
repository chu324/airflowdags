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

    // 分类统计失败情况
    private final Map<String, AtomicInteger> failureCategories = new HashMap<>();
    private final Map<String, List<String>> failureSdkFileIds = new HashMap<>();

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

    public void logFailureCategory(String category, String sdkfileid) {
        System.out.println("记录失败分类: " + category + ", sdkfileid: " + sdkfileid);
        failureCategories.computeIfAbsent(category, k -> new AtomicInteger(0)).incrementAndGet();
        failureSdkFileIds.computeIfAbsent(category, k -> new ArrayList<>()).add(sdkfileid);
    }

    public void logStatistics() {
        System.out.println("定时任务触发，正在输出统计信息...");
        System.out.println(String.format("[媒体文件下载统计] 总文件数=%d, 成功=%d, 失败=%d, 过期=%d",
                totalMediaFiles.get(), successCount.get(), failureCount.get(), expiredCount.get()));

        if (!failureCategories.isEmpty()) {
            System.out.println("失败分类统计:");
            for (Map.Entry<String, AtomicInteger> entry : failureCategories.entrySet()) {
                System.out.println("- " + entry.getKey() + ": " + entry.getValue().get());
            }
        }

        if (!failureSdkFileIds.isEmpty()) {
            System.out.println("失败的文件列表按类型分类:");
            for (Map.Entry<String, List<String>> entry : failureSdkFileIds.entrySet()) {
                System.out.println("类型: " + entry.getKey());
                for (String sdkfileid : entry.getValue()) {
                    System.out.println("  - " + sdkfileid);
                }
            }
        }
    }
}
