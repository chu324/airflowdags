package com.tencent.wework;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class SchedulerSetup {
    private static final Logger logger = Logger.getLogger(SchedulerSetup.class.getName());

    static {
        try {
            // 配置日志记录
            String logDir = "logs/"; // 修改为相对路径
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Shanghai"));
            String dateStr = now.format(formatter);
            String logPath = logDir + dateStr + "/scheduler.log";

            // 确保目录存在
            java.io.File logDirFile = new java.io.File(logDir);
            if (!logDirFile.exists()) {
                logDirFile.mkdirs(); // 创建目录
            }

            FileHandler fileHandler = new FileHandler(logPath, true);
            fileHandler.setFormatter(new SimpleFormatter());
            Logger.getLogger("com.tencent.wework").addHandler(fileHandler);
        } catch (IOException e) {
            System.err.println("配置日志记录失败: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws SchedulerException, InterruptedException {

        // 创建调度器
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

        // 定义任务
        JobDetail job = JobBuilder.newJob(FetchDataJob.class)
                .withIdentity("fetchDataJob", "group1")
                .build();

        // 定义触发器（北京时间2025年1月17日上午11点执行）
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("fetchDataTrigger", "group1")
                .withSchedule(CronScheduleBuilder.cronSchedule("0 12 18 * * ?")) // 0 05 0 * * ? 上午0点5分执行
                .build();

        // 绑定任务和触发器
        scheduler.scheduleJob(job, trigger);

        // 启动调度器
        scheduler.start();
        logger.info("调度器已启动");
        System.out.println("Current working directory: " + System.getProperty("user.dir"));

        // 让主线程保持运行
        Thread.sleep(Long.MAX_VALUE);
    }
}
