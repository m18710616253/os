package cn.ctyun.oos.server.stress;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.Program;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class PerformanceMonitor  implements Program {
	private static final Log LOG = LogFactory.getLog(PerformanceMonitor.class);
	private static final Log OUT = LogFactory.getLog("performLog");
	
	private static final int OBJECT_SIZE = 4 * 1024 *1024;

	private static String username;
	private static String password;
	private static String url;
	private static int runInter;
	private static int total;
	
	private static String objectName = "object-name-for-performance-test-6463084869102846986";
	private static String bucketName = "bucket-name-for-performance-test-7845589720135488896";
	
	private static AmazonS3 client = null;

	static{
		Properties p = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(System.getProperty("user.dir")
					+ "/conf/performanceTest.conf");
			p.load(is);
			username = p.getProperty("username");
			password = p.getProperty("password");
			url = p.getProperty("url");
			runInter = Integer.parseInt(p.getProperty("runInter"));
			total = Integer.parseInt(p.getProperty("times"));
			
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}
	
	static void init(){
		ClientConfiguration configuration = new ClientConfiguration();
		configuration.setConnectionTimeout(10000);
		configuration.setSocketTimeout(10000);
		try {
			client = new AmazonS3Client(new AWSCredentials() {
				public String getAWSAccessKeyId() {
					return username;
				}

				public String getAWSSecretKey() {
					return password;
				}
			}, configuration);
			client.setEndpoint(url);
		} catch (Exception e) {
			System.err.println("Init AmazonS3 Client Error!");
			System.exit(-1);
		}
	}
	
	@Override
	public String usage() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void exec(String[] args) throws Exception {
		init();
		Timer timer = new Timer();
		timer.schedule(new Task(), 0, runInter);
	}
	
	static class Task extends TimerTask{
		//一个账号最多可以创建十个bucket
		private static final int BUCKET_NUM = 10;
		private static Executor pool = Executors.newFixedThreadPool(BUCKET_NUM * 2);
		private static int n = 0;
		private static Set<Integer> nums = Collections.synchronizedSet(new HashSet<Integer>());

		@Override
		public void run() {
			final AtomicInteger count = new AtomicInteger(0);
			for(int i=0;i<total;i++){
				final int j = i;
				pool.execute(new Thread(){
					public void run(){
						try {
							int num = getNum();
							does(bucketName + "_" + num,objectName + "_" + j);
							nums.remove(num);
						} catch (Exception e) {
							//do nothing
							;
						}
						count.incrementAndGet();
					}
				});
			}
			while(count.get() < total)
				Thread.yield();
		}
		
		/**
		 * 测试创建bucket、上传object、获取object、删除object、删除bucket性能。如果在测试过程中出现任何异常产生的结果都是timeout，即在测试结果日志中写入xxxx-timeout
		 * @param bucketName
		 * @param objectName
		 * @throws Exception
		 */
		private static void does(String bucketName,String objectName) throws Exception{
			bucketCreate(bucketName);
			objectPut(bucketName, objectName);
			objectGet(bucketName,objectName);
			objectDelete(bucketName,objectName);
			bucketDelete(bucketName);
		}
		
		/**
		 * 上传object
		 * @param bucket
		 * @param object
		 * @throws Exception
		 */
		private static void objectPut(String bucket, String object) throws Exception{
			try {
				final Random r = new Random();
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(OBJECT_SIZE);
				final AtomicLong start = new AtomicLong(0);
				client.putObject(bucket, object, new InputStream() {
					int pos = 0;

					public int read() throws IOException {
						int data = (pos++ >= OBJECT_SIZE) ? -1 : (r.nextInt() & 0xFF);
						if(data == -1)
							start.set(System.currentTimeMillis());
						return data;
					}
				}, metadata);
				long end = System.currentTimeMillis();
				double time = (double)(end - start.get()) / 1000;
				StringBuilder text = new StringBuilder();
				text.append("objectPut-time-consuming:").append(time);
				OUT.info(text.toString());
				LOG.info("objectPut seccess");
			} catch (Exception e) {
				LOG.error("objectPut Error", e);
				OUT.info("objectPut-timeout");
				throw e;
			}
		}
		/**
		 * 获取object
		 * @param bucket
		 * @param object
		 * @throws Exception
		 */
		private static void objectGet(String bucket, String object) throws Exception {
			try {
				long start = System.currentTimeMillis();
				S3Object s3Object = client.getObject(bucket, object);
				S3ObjectInputStream is = s3Object.getObjectContent();
				while(is.read() != -1)
					;
				long end = System.currentTimeMillis();
				double time = (double)(end - start) / 1000;
				StringBuilder text = new StringBuilder();
				text.append("objectGet-time-consuming:").append(time);
				OUT.info(text.toString());
				LOG.info("objectGet seccess");
			} catch (Exception e) {
				LOG.error("objectGet Error", e);
				OUT.info("objectGet-timeout");
				throw e;
			}
		}
		/**
		 * 删除object
		 * @param bucket
		 * @param object
		 * @throws Exception
		 */
		private static void objectDelete(String bucket, String object) throws Exception{
			try {
				long start = System.currentTimeMillis();
				client.deleteObject(bucket, object);
				long end = System.currentTimeMillis();
				double time = (double)(end - start) / 1000;
				StringBuilder text = new StringBuilder();
				text.append("objectDelete-time-consuming:").append(time);
				OUT.info(text.toString());
				LOG.info("objectDelete seccess");
			} catch (Exception e) {
				LOG.error("objectDelete Error", e);
				OUT.info("objectDelete-timeout");
				throw e;
			}
		}
		/**
		 * 创建bucket
		 * @param bucket
		 * @throws Exception
		 */
		private static void bucketCreate(String bucket) throws Exception{
			try {
				long start = System.currentTimeMillis();
				if(!client.doesBucketExist(bucket))
					client.createBucket(bucket);
				long end = System.currentTimeMillis();
				double time = (double)(end - start) / 1000;
				StringBuilder text = new StringBuilder();
				text.append("bucketCreate-time-consuming:").append(time);
				OUT.info(text.toString());
				LOG.info("bucketCreate seccess");
			} catch (Exception e) {
				LOG.error("bucketCreate Error", e);
				OUT.info("bucketCreate-timeout");
				throw e;
			}
		}
		/**
		 * 删除bucket
		 * @param bucket
		 * @throws Exception
		 */
		public static void bucketDelete(String bucket) throws Exception{
			ObjectListing objects = client.listObjects(bucket);
			List<S3ObjectSummary> objSum = objects.getObjectSummaries();
			if(!objSum.isEmpty())
				for(S3ObjectSummary object:objSum)
					objectDelete(bucket, object.getKey());
			
			try {
				long start = System.currentTimeMillis();
				client.deleteBucket(bucket);
				long end = System.currentTimeMillis();
				double time = (double)(end - start) / 1000;
				StringBuilder text = new StringBuilder();
				text.append("bucketDelete-time-consuming:").append(time);
				OUT.info(text.toString());
				LOG.info("bucketDelete seccess");
			} catch (Exception e) {
				LOG.error("bucketDelete Error", e);
				OUT.info("bucketDelete-timeout");
				throw e;
			}
		}
		synchronized private static int getNum(){
			while(true){
				n++;
				if(n < 0)
					n = 0;
				if(!nums.contains(n % BUCKET_NUM)){
					nums.add(n % BUCKET_NUM);
					return n % BUCKET_NUM;
				}
			}
		}
	}
}
