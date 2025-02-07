package com.tencent.wework;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import java.util.logging.Logger;

public class FetchDataJob implements Job {
    private static final Logger logger = Logger.getLogger(FetchDataJob.class.getName());

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        long sdk = 0;
        long startTime = System.currentTimeMillis();
        String taskName = "FetchDataJob";
        String status = "success";
        String errorMsg = "";
        int dataSize = 0;

        try {
            // 重置任务日期
            FetchData.taskDateStr = null;

            // 初始化 SDK
            sdk = Finance.NewSdk();
            int initRet = Finance.Init(sdk, "wx1b5619d5190a04e4", "qY6ukRvf83VOi6ZTqVIaKiz93_iDbDGqVLBaSKXJCBs");
            if (initRet != 0) {
                logger.severe("SDK 初始化失败, ret: " + initRet);
                status = "failed";
                errorMsg = "SDK 初始化失败, ret: " + initRet;
                return;
            }

            // 调用拉取数据的逻辑
            boolean hasMoreData = FetchData.fetchNewData(sdk);
            if (!hasMoreData) {
                logger.info("所有数据已拉取完成！");
            }

            // 假设 dataSize 为拉取的数据量
            dataSize = 100; // 这里可以根据实际情况设置

        } catch (Exception e) {
            // 记录错误日志
            logger.severe("拉取数据失败: " + e.getMessage());
            status = "failed";
            errorMsg = e.getMessage();
            // 发送告警通知
            sendAlert("拉取数据失败，请检查！");
        } finally {
            // 销毁 SDK
            if (sdk != 0) {
                Finance.DestroySdk(sdk);
            }

            // 记录任务状态
            long taskDuration = System.currentTimeMillis() - startTime;
            MonitorLog.logTaskStatus(taskName, status, errorMsg, dataSize, taskDuration);
        }
    }

    private void sendAlert(String message) {
        // 实现告警通知逻辑（如发送邮件、短信等）
        logger.info("发送告警: " + message);
    }
}
