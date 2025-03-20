package com.tencent.wework;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.logging.Logger;

public class SchedulerSetup {
    private static final Logger logger = Logger.getLogger(SchedulerSetup.class.getName());

    public static void main(String[] args) {
        try {
            // 1. 创建调度器实例
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

            // 2. 定义任务（JobDetail）
            JobDetail job = newJob(FetchDataJob.class)
                .withIdentity("fetchDataJob", "group1")
                .build();

            // 3. 定义触发器（Trigger），每分钟执行一次
            Trigger trigger = newTrigger()
                .withIdentity("trigger1", "group1")
                .startNow()
                .withSchedule(cronSchedule("0 * * * * ?")) // 每分钟执行一次
                .build();

            // 4. 将任务和触发器注册到调度器
            scheduler.scheduleJob(job, trigger);

            // 5. 启动调度器
            scheduler.start();
            logger.info("调度器已启动，任务将每分钟执行一次");

            // 保持主线程运行
            Thread.sleep(Long.MAX_VALUE);

        } catch (SchedulerException e) {
            logger.severe("调度器初始化失败: " + e.getMessage());
        } catch (InterruptedException e) {
            logger.severe("主线程被中断: " + e.getMessage());
        }
    }
}
