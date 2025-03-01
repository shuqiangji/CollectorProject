package com.mobin.collector;

import com.mobin.common.DataFile;
import com.mobin.config.HdfsConfig;
import jodd.datetime.TimeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by Mobin on 2017/5/6.
 * 实现ThreadFactory定制守护线程，之所以使用守护线程是因为采集程序需要一个长驻的不断轮询的线程
 */
public class FSUtils {

    private static final Logger log = LoggerFactory.getLogger(FSUtils.class);

    private static ThreadPoolExecutor threadPoolExecutor;

    public static synchronized ThreadPoolExecutor getThreadPoolExecutor(){
        return getThreadPoolExecutor(-1);
    }

    public static synchronized ThreadPoolExecutor getThreadPoolExecutor(int maximumPoolSize){
        if (threadPoolExecutor == null) {
            threadPoolExecutor = createThreadPoolExecutor(maximumPoolSize);
        }
        return threadPoolExecutor;
    }

    public static ThreadPoolExecutor createThreadPoolExecutor(int maximumPoolSize){
        if (maximumPoolSize <= 0)
            //采集任务为I/O密集型任务，任务执行过程中等待I/O的时间长于使用CPU的时间，且牌I/O等待状态的时间的线程并不会消耗CPU资源，所以maximumPoolSize=availableProcessors*2
            maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new DeamonThreadFactory());
        tpe.allowsCoreThreadTimeOut();
        return tpe;
    }

    public static VolatileExecutor createVolatileExecutor(String name) {
        return createVolatileExecutor(name, -1);
    }

    public static VolatileExecutor createVolatileExecutor(String name, int maximumPoolSize){
        if (maximumPoolSize <= 0) {
            maximumPoolSize = Runtime.getRuntime().availableProcessors();
        }
        return new VolatileExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                new DeamonThreadFactory(name));
    }

    public static class VolatileExecutor implements AutoCloseable{
        private final ArrayList<Future<?>> futures = new ArrayList<>();
        private final ArrayList<Object> tasks = new ArrayList<>();
        private  final ThreadPoolExecutor threadPoolExecutor;

        public VolatileExecutor(int corePoolSize,   //核心线程，池中所保存的线程数
                                int maximumPoolSize,         //最大线程数，可创建的最大线程数
                                long keepAileTime,              //如果线程数大于corePoolSize,则这些多余的线程空闲时间超过该参数将被终止
                                TimeUnit unit,                      //keepAileTime的时间单位
                                BlockingQueue<Runnable> workQueue,   //保存任务的阻塞队列
                                ThreadFactory threadFactory){
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAileTime, unit, workQueue, threadFactory);
            threadPoolExecutor.setRejectedExecutionHandler(blockingExecutorHandler);
            threadPoolExecutor.allowsCoreThreadTimeOut();

        }

        @Override
        public void close() throws Exception {
              if (!futures.isEmpty()) {
                  await();
              }
            threadPoolExecutor.shutdown();
        }

        //传入task，要把task的类型来执行任务
        public void submitTasks(List<?> tasks) {
            for (Object task : tasks) {
                if (task instanceof  Runnable) {
                    submitTask((Runnable) task);
                }else if (task instanceof  Callable) {
                    submitTask((Callable<?>) task);
                }else {
                    log.warn("Invalid task: " + task);
                }
            }
        }

        public void submitTask(Runnable task){
            try {
                futures.add(threadPoolExecutor.submit(task));   // 方便获取任务执行结果
                tasks.add(task);
            }catch (Exception e){
                log.info("Failed to submit tabks " + task + "," + e);
            }
        }

        public void submitTask(Callable<?> task) {
            try {
                futures.add(threadPoolExecutor.submit(task));
                tasks.add(task);
            }catch (Exception e) {
                log.error("Failed to submit task: " + task + "," + e);
            }
        }

        public void await() {
            for (int i = 0, size = futures.size(); i < size; i  ++) {
                try {
                    futures.get(i).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            futures.clear();
            tasks.clear();
        }
    }

    //定义被拒绝处理任务的策略
    public static final RejectedExecutionHandler blockingExecutorHandler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (true) {
                if (executor.isShutdown()) {
                    throw  new RejectedExecutionException("TheadPoolExecutor has shut down!");
                }
                try {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS)){
                        break;
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }

        }
    };

    public static class DeamonThreadFactory implements  ThreadFactory {
        private final String id;
        private final int priority;
        private final AtomicInteger n = new AtomicInteger(1);

        public DeamonThreadFactory(){
            this.id = "mobin-thread";
            this.priority = Thread.NORM_PRIORITY;
        }

        public DeamonThreadFactory(String id) {
            this(id, Thread.NORM_PRIORITY);
        }

        public DeamonThreadFactory(String id, int priority){
            this.id = "mobin" + id + "-thread";
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            String name = id + "-" + n.getAndIncrement();
            Thread thread = new Thread(runnable,name);
            thread.setPriority(priority);
            thread.setDaemon(true);    //守护线程
            return thread;
        }
    }

    public static BufferedReaderIterable createBufferedReadIterable(FileSystem fs, String file) throws IOException {
        return new BufferedReaderIterable(fs, file);
    }

    public static class BufferedReaderIterable implements Iterable<String>, Closeable {

        private final long startTime = System.currentTimeMillis();
        private final String file;
        private final BufferedReader br;
        private final long size;
        private long vaildRecords;

        public BufferedReaderIterable(FileSystem fs, String file) throws IOException {
            this.file = file;
            Path path = new Path(file);
            this.size = fs.getFileStatus(path).getLen();

            //解压缩
//            CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
            CompressionCodec codec = null;

            FSDataInputStream inputStream = fs.open(path, 8096);

            if (codec == null) {//文件没有进行压缩
                br = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
            } else {
                //创建compressionInputStream来对文件进行解压
                CompressionInputStream compressionInputStream = codec.createInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(compressionInputStream));
            }

        }

        public void incrementVaildRecords() {
            vaildRecords ++;
        }

        public long getVaildRecords() {
            return vaildRecords;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private String line;
                @Override
                public boolean hasNext() {
                    try {
                        line = br.readLine();
                    }catch (IOException e) {
                        log.error("Failed to readLine, file: " + file, e);
                        line = null;
                    }
                    return line != null;
                }

                @Override
                public String next() {
                    return line;
                }

                @Override
                public void remove() {
                       throw new UnsupportedOperationException("remove");
                }
            };
        }

        @Override
        public void close() throws IOException {
            if (br != null) {
                try{
                    br.close();
                }catch (Throwable t){
                    log.debug("Failed to close stream");
                }

            }
        }
    }

    public static String appendSlash(String str) {
        if (str == null) {
            return null;
        }
        if (!str.endsWith(File.separator)) {
            str = str + File.separator;
        }
        return str;
    }

    public static String getDate(String dateTime) {
        return dateTime.substring(0, dateTime.length() -2);
    }

    public static String getCurrentDate(SimpleDateFormat dateFormat) {
        synchronized (dateFormat) {
            return dateFormat.format(new Date());
        }
    }

    //计算
    public static List<String> getDates(String startDate, String endDate, SimpleDateFormat simpleDateFormat) throws ParseException {
        List<String> dates = new ArrayList<>();
        Date start = parseDate(startDate, simpleDateFormat);
        Date end = parseDate(endDate, simpleDateFormat);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        while (calendar.getTime().compareTo(end) <= 0) {
            dates.add(formateDate(calendar.getTime(), simpleDateFormat));
            calendar.add(Calendar.DAY_OF_MONTH, 1);
        }

        return dates;
    }

    public static Date parseDate(String date,SimpleDateFormat dateFormat) throws ParseException {
        synchronized (dateFormat) {
            return dateFormat.parse(date);
        }
    }

    public static long parseDateTime(String dateTime, SimpleDateFormat dateTimeFormat) throws ParseException {
        synchronized (dateTimeFormat){
            return dateTimeFormat.parse(dateTime).getTime();
        }
    }

    public static String formateDate (Date date, SimpleDateFormat dateFormat) {
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    public static String getUID() {
        String ip;
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "UnknownHost";
            }
            ip += "_" + UUID.randomUUID();
            ip = ip.replace(".","_").replace(":","_").replace("-","_");
            return ip;
    }

    public static OutputStream openOutputStream(FileSystem fs, Path path) throws IOException {
        OutputStream os = null;
        if (fs.exists(path)) {
            try {
                os = fs.append(path);
            } catch (Exception e) {
                //不支持append
                byte[] oldBytes = FSUtils.readDataFile(fs ,path);
                os = fs.create(path);  //打开path文件获取流
                os.write(oldBytes);
            }
        } else {
            os = fs.create(path);
        }
          return os;
        }

    public static byte[] readDataFile(FileSystem fs, Path dateFile) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2 * 1024 * 1024);
        InputStream in = null;
        try {
            in = fs.open(dateFile);
            IOUtils.copyBytes(in, bos, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }

        return bos.toByteArray();
    }

    public static void closeStreamSilently(Closeable stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Throwable e) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to close stream", e);
                }
            }
        }
    }

    public static boolean exists(FileSystem fs, String p) throws IOException {
        return fs.exists(new Path(p));
    }

    public static boolean isFile(FileSystem fs,String p) throws IOException {
        return fs.isFile(new Path(p));
    }

    public static FileSystem getFileSystem() throws IOException {
        //Configuration configuration = new Configuration();
        Configuration configuration = getConfiguration();
        return FileSystem.get(configuration);
    }

    public static void getDataFileRecurslvely(FileSystem fs, String p, ArrayList<DataFile> files) throws IOException {
        getDataFileRecurslvely(fs, new Path(p), files);
    }

    //遍历lzo文件
    public static void getDataFileRecurslvely(FileSystem fs, Path p, ArrayList<DataFile> files) throws IOException {
        if (fs.isFile(p)){
            if (isNotEmptyFile(fs, p))
                files.add(new DataFile(p.toString()));
            return;
        }

        boolean containsLzo = false;
        for (FileStatus fileStatus: fs.listStatus(p)){
            Path path = fileStatus.getPath();
            if(fs.isFile(path) && DataFile.isLzoFile(path)){
                containsLzo = true;
                break;
            }
        }

        //如果当前目录下有lzo文件，那么不再递归遍历子目录，并且只抽取lzo文件
        if (containsLzo){
            for (FileStatus fileStatus: fs.listStatus(p)){
                Path lzoPath = fileStatus.getPath();
                if (fs.isFile(lzoPath) && DataFile.isLzoFile(lzoPath) && isNotEmptyFile(fs, lzoPath)){
                    files.add(new DataFile(lzoPath, true));
                }
            }
        }else {
            for (FileStatus fileStatus: fs.listStatus(p)){
                Path path = fileStatus.getPath();
                if (fs.isFile(path) && isNotEmptyFile(fs, path)){
                    files.add(new DataFile(path, false));
                }else{
                    getDataFileRecurslvely(fs,path,files);
                }
            }
        }
    }

    public static boolean isNotEmptyFile(FileSystem fs,Path file) throws IOException {
        FileStatus fileStatus = fs.getFileStatus(file);
        return fileStatus.getLen() > 0;
    }
    
    /**
     * 获取HDFS配置信息
     * 
     * @author JSQ
     * @param
     * @return {@link Configuration}
     */
    private static Configuration getConfiguration(){
        if (StringUtils.isNotEmpty(HdfsConfig.hdfsUser)) {
            System.setProperty("HADOOP_USER_NAME", HdfsConfig.hdfsUser);
        }
        return HdfsConfig.isHa? getHaConfiguration() : getSimpleConfiguration();
    }

    /**
     * 获取HDFS配置信息，HA模式
     */
    private static Configuration getHaConfiguration(){
        if (StringUtils.isEmpty(HdfsConfig.nameServices) || CollectionUtils.isEmpty(HdfsConfig.nameNodes)
                || CollectionUtils.isEmpty(HdfsConfig.nameNodesAddress)
                || HdfsConfig.nameNodes.size() != HdfsConfig.nameNodesAddress.size()) {
            throw new RuntimeException("HDFS配置不正确，请检查！");
        }
        Configuration conf = new Configuration();
        String defaultFs = "hdfs://" + HdfsConfig.nameServices;
        conf.set("fs.defaultFS", defaultFs);
        conf.set("dfs.nameservices", HdfsConfig.nameServices);
        String nameNodes = "dfs.ha.namenodes." + HdfsConfig.nameServices;
        conf.set(nameNodes, StringUtils.join(HdfsConfig.nameNodes, ","));
        for (int i = 0; i < HdfsConfig.nameNodes.size(); i++) {
            String key = "dfs.namenode.rpc-address." + HdfsConfig.nameServices + "." + HdfsConfig.nameNodes.get(i);
            conf.set(key, HdfsConfig.nameNodesAddress.get(i));
        }
        conf.set("dfs.client.failover.proxy.provider." + HdfsConfig.nameServices,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return conf;
    }

    /**
     * 获取HDFS配置信息，伪分布式模式
     */
    private static Configuration getSimpleConfiguration(){
        if (StringUtils.isEmpty(HdfsConfig.nameServices)) {
            throw new RuntimeException("HDFS配置不正确，请检查！");
        }
        Configuration conf = new Configuration();
        String defaultFs = "hdfs://" + HdfsConfig.nameServices;
        conf.set("fs.defaultFS", defaultFs);
        return conf;
    }

}
