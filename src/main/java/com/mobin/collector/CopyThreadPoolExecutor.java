package com.mobin.collector;

import org.apache.tools.ant.taskdefs.optional.extension.LibFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author JSQ
 * @date 8/18/2021 1:49 PM
 * @description 拷贝线程池
 * @copyright 2021  All rights reserved.
 */
public class CopyThreadPoolExecutor {

    private final static Logger LOGGER = LoggerFactory.getLogger(CopyThreadPoolExecutor.class);

    /**
    * 存放线程池submit结果
    */
    private final List<Future<?>> futures = new CopyOnWriteArrayList<>();

    /**
    * 存放任务
    */
    private final List<Object> tasks = new CopyOnWriteArrayList<>();

    private static final ThreadPoolExecutor EXECUTOR =
            new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 2,
                    3L, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>(),
                    new FSUtils.DeamonThreadFactory());

    static {
        EXECUTOR.allowCoreThreadTimeOut(true);
    }

    /**
     * 注意，这里是个static，也就是只能有一个线程池。用户自定义线程池时，也只能定义一个
     */
    private static ExecutorService executorService;

    //传入task，要把task的类型来执行任务
    public void submitTasks(List<?> tasks) {
        for (Object task : tasks) {
            if (task instanceof Runnable) {
                submitTask((Runnable) task);
            } else if (task instanceof Callable) {
                submitTask((Callable<?>) task);
            } else {
                LOGGER.warn("Invalid task: " + task);
            }
        }
    }

    public void submitTask(Runnable task) {
        try {
            futures.add(EXECUTOR.submit(task));   // 方便获取任务执行结果
            tasks.add(task);
        } catch (Exception e) {
            LOGGER.info("Failed to submit tabks " + task + "," + e);
        }
    }

    public void submitTask(Callable<?> task) {
        try {
            futures.add(EXECUTOR.submit(task));
            tasks.add(task);
        } catch (Exception e) {
            LOGGER.error("Failed to submit task: " + task + "," + e);
        }
    }


    public void await() {
        for (int i = 0, size = futures.size(); i < size; i++) {
            try {
                futures.get(i).get();
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
        futures.clear();
        tasks.clear();
    }

    /**
     * 关闭线程池
     */
    public static void shutDown(ExecutorService executorService) {
        if (executorService != null) {
            executorService.shutdown();
        } else {
            EXECUTOR.shutdown();
        }
    }

    public static String getThreadCount() {
        return "activeCount=" + EXECUTOR.getActiveCount() +
                "  completedCount " + EXECUTOR.getCompletedTaskCount() +
                "  largestCount " + EXECUTOR.getLargestPoolSize();
    }
}
