// 日志聚合器类
public static class LogAggregator {
    private final AtomicInteger totalMediaFiles = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger expiredCount = new AtomicInteger(0);
    private final List<String> failureDetails = Collections.synchronizedList(new ArrayList<>());

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

    public void addFailureDetail(String detail) {
        failureDetails.add(detail);
    }

    public void logStatistics() {
        logger.info(String.format("[媒体文件下载统计] 总文件数=%d, 成功=%d, 失败=%d, 过期=%d",
                totalMediaFiles.get(), successCount.get(), failureCount.get(), expiredCount.get()));
        if (!failureDetails.isEmpty()) {
            logger.info("失败的文件列表:");
            for (String detail : failureDetails) {
                logger.info("- " + detail);
            }
        }
    }
}
