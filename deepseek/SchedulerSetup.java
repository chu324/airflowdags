package com.tencent.wework;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import static org.quartz.CronScheduleBuilder.*;
import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import java.util.TimeZone;
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
            // 配置日志记录到按日期分目录的日志文件
            String logDir = "logs/";
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Shanghai"));
            String dateStr = now.format(dateFormatter);
            String logPath = logDir + dateStr + "/scheduler.log";
            
            // 确保日志目录存在
            java.nio.file.Path logDirPath = java.nio.file.Paths.get(logDir + dateStr);
            if (!java.nio.file.Files.exists(logDirPath)) {
                java.nio.file.Files.createDirectories(logDirPath);
            }

            // 初始化文件日志处理器
            FileHandler fileHandler = new FileHandler(logPath, true);
            fileHandler.setFormatter(new SimpleFormatter());
            Logger.getLogger("com.tencent.wework").addHandler(fileHandler);
        } catch (IOException e) {
            System.err.println("日志配置失败: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        Scheduler scheduler = null;
        try {
            // 1. 创建调度器实例
            scheduler = StdSchedulerFactory.getDefaultScheduler();

            // 2. 定义任务（JobDetail）
            JobDetail job = newJob(FetchDataJob.class)
                .withIdentity("fetchDataJob", "group1")
                .build();

            // 3. 定义触发器：每天北京时间20:13执行一次
            Trigger trigger = newTrigger()
                .withIdentity("trigger1", "group1")
                .withSchedule(cronSchedule("0 13 20 * * ?")
                    .inTimeZone(TimeZone.getTimeZone("Asia/Shanghai")))
                .build();

            // 4. 注册任务和触发器
            scheduler.scheduleJob(job, trigger);

            // 5. 注册优雅关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (scheduler != null && scheduler.isStarted()) {
                        scheduler.shutdown(true); // true表示等待任务完成
                        logger.info("调度器已关闭");
                    }
                } catch (SchedulerException e) {
                    logger.severe("调度器关闭失败: " + e.getMessage());
                }
            }));

            // 6. 启动调度器
            scheduler.start();
            logger.info("调度器已启动，任务将在每天20:13执行");

            // 7. 保持主线程运行（非阻塞式）
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(Long.MAX_VALUE); // 避免忙等待
            }

        } catch (SchedulerException e) {
            logger.severe("调度器初始化失败: " + e.getMessage());
        } catch (InterruptedException e) {
            logger.warning("主线程被中断");
            Thread.currentThread().interrupt();
        } finally {
            if (scheduler != null) {
                try {
                    if (!scheduler.isShutdown()) {
                        scheduler.shutdown(false); // 确保最终关闭
                    }
                } catch (SchedulerException e) {
                    logger.severe("最终关闭调度器失败: " + e.getMessage());
                }
            }
        }
    }
}
