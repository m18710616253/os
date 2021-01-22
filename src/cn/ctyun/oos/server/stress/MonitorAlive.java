package cn.ctyun.oos.server.stress;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import cn.ctyun.common.Program;
import cn.ctyun.common.utils.MailUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import common.util.MD5Hash;

public class MonitorAlive implements Program {
	final static int length = 16 * 1024;
	/*
	 * private static String from = "ctcloudcomputing@163.com"; private static
	 * String smtpHost = "smtp.163.com"; private static String pwd =
	 * "ctyun.product"; private static int portNumber = 25;
	 */
	private static String nick;
	private static int runInter;
	private static int sendEmailInter;
	private static String objectName = "object-name-for-monitor-test2-6463084869102846986";
	private static String bucketName = "bucket-name-for-monitor-test-7845589720135488896";

	private static String url;
	private static String username;
	private static String password;
	private static Map<String, String> objectNames = new HashMap<String, String>();
	private static String[] oosDatabase;
	/*
	 * private static String smtpHost = "mail1.ctyun.cn"; private static int
	 * portNumber = 25; private static String smtpUserName = "no-reply"; private
	 * static String smtpPwd = "ctyun.com.cn"; private static String email = ;
	 * private static String nickName = "hello"; private static String[] to;
	 */

	private static Log log = LogFactory.getLog(MonitorAlive.class);
	static {
		Properties p = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(System.getProperty("user.dir")
					+ "/conf/TestAlive.conf");
			p.load(is);
			username = p.getProperty("username");
			password = p.getProperty("password");
			url = p.getProperty("url");
			nick = p.getProperty("nick");
			runInter = Integer.parseInt(p.getProperty("runInter"));
			sendEmailInter = Integer.parseInt(p.getProperty("sendEmailInter"));

			String[] objNames = p.getProperty("objectName_CD").split(",");
			String[] tmp;
			for (String objName : objNames) {
				tmp = objName.split(":");
				objectNames.put(tmp[0], tmp[1]);
			}
			oosDatabase = p.getProperty("oosDatabase").split(";");
		} catch (FileNotFoundException e) {
			log.error(e.getMessage(), e);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	static class TaskCD extends TimerTask {
		private static long lastSendAlive = System.currentTimeMillis();
		
		private static long oneDay = 86400000;
		private static final int TOTAL = 15;
		private static final int BUCKET_NUM = 10;
		private static Executor pool = Executors
				.newFixedThreadPool(BUCKET_NUM * 2);
		private static DateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		private final static Map<String, Boolean> LAST_CD = new ConcurrentHashMap<String, Boolean>();
		private final static Map<String, Boolean> SEND_ABNORMAL_CD = new ConcurrentHashMap<String, Boolean>();
		private final static Map<String, Boolean> NORMAL_FLAG_CD = new ConcurrentHashMap<String, Boolean>();
		private final static Map<String, Long> LAST_SEND_ALERT = new ConcurrentHashMap<String, Long>();

		private static Object lock = new Object();

		static {
			Set<String> keys = objectNames.keySet();
			Iterator<String> itr = keys.iterator();
			while (itr.hasNext()) {
				String key = itr.next();
				LAST_CD.put(key, true);
				SEND_ABNORMAL_CD.put(key, false);
				NORMAL_FLAG_CD.put(key, true);
				LAST_SEND_ALERT.put(key, System.currentTimeMillis());
			}
		}

		@Override
		public void run() {
			ClientConfiguration configuration = new ClientConfiguration();
			/*configuration.setConnectionTimeout(10000);
			configuration.setSocketTimeout(10000);*/
			configuration.setMaxErrorRetry(0);
			AmazonS3 asc = null;
			try {
				asc = new AmazonS3Client(new AWSCredentials() {
					public String getAWSAccessKeyId() {
						return username;
					}

					public String getAWSSecretKey() {
						return password;
					}
				}, configuration);
				asc.setEndpoint(url);
			} catch (Exception e) {
				log.error(nick + "::" + e.getMessage(), e);
				throw new Error(e);
			}
			// StringBuilder body = new StringBuilder();
			Set<String> keys = objectNames.keySet();
			Iterator<String> itr = keys.iterator();
			final Set<Integer> buckets = Collections
					.synchronizedSet(new HashSet<Integer>());
			final AmazonS3 client = asc;
			final AtomicInteger complete = new AtomicInteger(0);
			while (itr.hasNext()) {
				final String key = itr.next();
				final String value = objectNames.get(key);
				final String object = objectName + "_" + key;

				new Thread() {
					public void run() {
						final AtomicInteger count = new AtomicInteger(0);
						final AtomicInteger bdCount = new AtomicInteger(0);
						long startTime = System.currentTimeMillis();
						boolean flag = true;
						String body = null;
						String subject = null;
						int j = 0;
						int m = j % BUCKET_NUM;
						synchronized (lock) {
							while (buckets.contains(m)) {
								m = ++m % BUCKET_NUM;
								m = m < 0 ? 0 : m;
							}
							buckets.add(m);
						}
						try {
							String bucket = bucketName + m;
							does(client, bucket, object);
							log.info(nick + "_" + value
									+ " Try first time success!");
						} catch (Exception e1) {
							log.info(nick + "_" + value
									+ " Try first time error!");
							log.error(e1.getMessage(), e1);
							if (!checkNetstat())
								bdCount.incrementAndGet();
							count.incrementAndGet();
							LAST_CD.put(key, false);

						}
						buckets.remove(m);
						if (!LAST_CD.get(key)) {
							final AtomicInteger n = new AtomicInteger(1);
							final AtomicInteger num = new AtomicInteger(1);
							for (int i = 1; i < TOTAL; i++) {
								pool.execute(new Thread() {
									public void run() {
										int j = num.getAndIncrement();
										int m = j % BUCKET_NUM;
										synchronized (lock) {
											while (buckets.contains(m)) {
												m = ++m % BUCKET_NUM;
												m = m < 0 ? 0 : m;
											}
											buckets.add(m);
										}
										try {
											String bucket = bucketName + m;
											does(client, bucket, object);
											log.info(nick + "_" + value
													+ " Try " + j
													+ " time success!");
										} catch(Exception e2) {
											log.info(nick + "_" + value
													+ " Try " + j
													+ " time error:");
											log.error(nick + "_" + value + "::"
													+ e2.getMessage(), e2);
											if (!checkNetstat())
												bdCount.incrementAndGet();
											count.incrementAndGet();
										}
										buckets.remove(m);
										n.incrementAndGet();
									}
								});
							}
							while (n.get() < TOTAL)
                                try {
                                    Thread.sleep(200);
                                } catch (InterruptedException e) {
                                    ;
                                }
							long endTime = System.currentTimeMillis();
							long time = (endTime - startTime) / 1000;
							if (count.get() > TOTAL * 80 / 100) {
								subject = "Status: Fatal";
								body = buildBody(value, 0, nick, time, TOTAL,
										count.get(), bdCount.get());
								NORMAL_FLAG_CD.put(key, false);
								flag = false;
							} else if (count.get() > TOTAL * 50 / 100) {
								subject = "Status: Error";
								body = buildBody(value, 1, nick, time, TOTAL,
										count.get(), bdCount.get());
								NORMAL_FLAG_CD.put(key, false);
								flag = false;
							} else if (count.get() > TOTAL * 20 / 100) {
								subject = "Status: Warning";
								body = buildBody(value, 2, nick, time, TOTAL,
										count.get(), bdCount.get());
								NORMAL_FLAG_CD.put(key, false);
								flag = false;
							} else {
								subject = "Status: OK";
								body = buildBody(value, 3, nick, time, TOTAL,
										count.get(), bdCount.get());
								LAST_CD.put(key, true);
							}
						}

						if (!NORMAL_FLAG_CD.get(key)) {
							lastSendAlive = System.currentTimeMillis();
							if (System.currentTimeMillis()
									- LAST_SEND_ALERT.get(key) >= sendEmailInter) {
								MailUtils.sendMail(subject, nick + " Monitor",
										body);
								LAST_SEND_ALERT.put(key,
										System.currentTimeMillis());
								SEND_ABNORMAL_CD.put(key, true);
							}
							if (flag) {
								if (SEND_ABNORMAL_CD.get(key)) {
									MailUtils.sendMail(subject, nick
											+ " Monitor", body);
									NORMAL_FLAG_CD.put(key, true);
									SEND_ABNORMAL_CD.put(key, false);
								}
								NORMAL_FLAG_CD.put(key, true);
							}

						} 
						complete.getAndIncrement();
					}
				}.start();
			}
			while (complete.get() < keys.size()) {
				try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    ;
                }
			}
			if (System.currentTimeMillis() - lastSendAlive >= oneDay) {
				String subject = "Status: OK";
				String body = buildBody(null, 4, nick, 0, 0, 0, 0);
				MailUtils.sendMail(subject, nick + " Monitor",
						body);
				lastSendAlive = System.currentTimeMillis();
			}

		}

		private static String buildBody(String num, int type, String nick,
				long time, int total, int failure, int bdFailure) {
			StringBuilder sb = new StringBuilder(
					"<table width=\"358\" border=\"0\" cellspacing=\"5\" style=\"font-size:14px\">")
					.append("<tr>")
					.append("<td width=\"225\" align=\"left\" valign=\"middle\">监控源 ：</td>")
					.append("<td width=\"137\" align=\"left\" valign=\"middle\">");
			if (type == 0) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
						.append(num).append("</td>").append("</tr>");
				sb.append("<tr>")
						.append("<td>报警级别：</td>")
						.append("<td bgcolor=\"#FFFF33\"><strong style=\"color:#C63\">致命</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>超过百分之八十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 1) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
						.append(num).append("</td>").append("</tr>");
				sb.append("<tr>")
						.append("<td>报警级别：</td>")
						.append("<td bgcolor=\"#FFFF33\"><strong>严重</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>超过百分之五十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 2) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
						.append(num).append("</td>").append("</tr>");
				sb.append("<tr>")
						.append("<td>报警级别：</td>")
						.append("<td bgcolor=\"#FFFF33\"><strong>警告</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>超过百分之二十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 3) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
						.append(num).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>报警级别：</td>")
						.append("<td><strong>恢复正常</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>不超过百分之二十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 4) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>报警级别：</td>")
						.append("<td><strong>正常</strong></td>").append("</tr>");
				sb.append("<tr>").append("<td>持续时间：</td>")
						.append("<td>过去的24小时</td>").append("</tr>");
			}
			sb.append("<tr>").append("<td>邮件发送时间：</td>").append("<td>")
					.append(format.format(new Date())).append("</td>")
					.append("</tr>");
			sb.append("<tr>").append("<td>监控数据库与所在机器对应关系：</td>").append("<td>");
			for(String ss : oosDatabase)
				sb.append(ss).append("<br/>");
			sb.append("</td>").append("</tr>");
			
			sb.append("</table>");
			sb.append("<p><em>注：</em></p>");
			sb.append(
					"<table width=\"653\" height=\"284\" border=\"1\" style=\"font-size:13px\">")
					.append("<tr>")
					.append("<td width=\"80\" rowspan=\"6\">报警级别 </td>")
					.append("<td width=\"563\">正常/恢复正常 &lt; 警告 &lt; 严重 &lt; 致命</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>正常（在过去的24小时中，OOS的状态都是正常的）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>恢复正常（在上次监控的失败率 &gt; 20%情况下，本次监控的失败率 &lt; 20%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>警告（失败率 &gt; 20%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>严重（失败率 &gt; 50%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>致命（失败率 &gt; 80%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>监控原则</td>")
					.append("<td><p>&nbsp;&nbsp;&nbsp;&nbsp;监控程序每30s运行一次，每次监控都至少访问30次OOS服务，使得一次OOS服务能对应到一台目标服务器，如果第一次访问OOS服务时出现异常，监控程序会在较短的时间内并发的访问29次OOS，然后进行统计分析，并根据不同的失败率发送不同级别的报警邮件；如果上次运行监控程序时出现失败率 &gt; 20%的情况，则在本次监控中会无条件的在较短的时间内并发的访问14次OOS，然后进行统计分析，如果失败率 &lt; 20%，则会发生恢复正常邮件，否则判断上次发送报警邮件的时间是否已经超过十分钟，如果超过则继续发送报警邮件，否则不发送；如果第一次访问OOS服务成功，监控程序等待下一次运行；如果监控程序已经超过24小时未出现失败率 &lt; 20%的情况，则会发送一封正常邮件。</p><p>&nbsp;&nbsp;&nbsp;&nbsp;在发送警告/严重/致命邮件之前，都会判断上次发送此类级别邮件的时间是否已经超过10分钟，只有时间间隔超过10分钟，才会再次发送此类级别的报警邮件。</p></td>")
					.append("</tr>").append("</table>");

			return sb.toString();
		}

	}

	private static boolean checkNetstat() {
		boolean flag = true;
		try {
			HttpGet hg = new HttpGet("http://www.baidu.com");
			HttpResponse response = new DefaultHttpClient().execute(hg);
			if (response.getStatusLine().getStatusCode() != 200)
				throw new RuntimeException();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			HttpGet hg = new HttpGet("http://www.sina.com.cn");
			HttpResponse response;
			try {
				response = new DefaultHttpClient().execute(hg);
				if (response.getStatusLine().getStatusCode() != 200)
					throw new RuntimeException();
			} catch (Exception e2) {
				log.error(e2.getMessage(), e2);
				flag = false;
			}
		}
		return flag;
	}

	private static void inition(AmazonS3 asc, String bucket, String object)
			throws Exception {
		if (!asc.doesBucketExist(bucket)) {
			CreateBucketRequest createBucketRequest = new CreateBucketRequest(
					bucket);
			createBucketRequest
					.setCannedAcl(CannedAccessControlList.PublicRead);
			asc.createBucket(createBucketRequest);
		}
		objectPut(asc, bucket, object);
	}

	private static void unInition(AmazonS3 asc, String bucket, String object)
			throws Exception {
		objectDelete(asc, bucket, object);
		ObjectListing list = asc.listObjects(bucket);
		List<S3ObjectSummary> objects = list.getObjectSummaries();
		if (!objects.isEmpty()) {
			for(S3ObjectSummary obj:objects)
				objectDelete(asc, bucket, obj.getKey());
		}
		//asc.deleteBucket(bucket);
	}

	private static void objectPut(AmazonS3 asc, String bucket, String object)
			throws Exception {
		final Random r = new Random();
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(length);
		asc.putObject(bucket, object, new InputStream() {
			int pos = 0;

			public int read() throws IOException {
				return (pos++ >= length) ? -1 : (r.nextInt() & 0xFF);
			}
		}, metadata);
		log.info(nick + "::" + "Put seccess");
	}

	private static boolean objectGet(AmazonS3 asc, String bucket, String object)
			throws Exception {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, object);
		S3Object s3Object = new S3Object();
		s3Object = asc.getObject(getObjectRequest);
		if (s3Object != null) {
			InputStream is = s3Object.getObjectContent();
			try {
				while (is.read() != -1)
					;
			} finally {
				is.close();
			}
			log.info(nick + "::" + "Get success at " + getDate());
			return true;
		}
		return false;
	}

	private static void objectDelete(AmazonS3 asc, String bucket, String object)
			throws Exception {
		DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(
				bucket, object);
		asc.deleteObject(deleteObjectRequest);
		log.info(nick + "::" + "Delete success");
	}

	private static String getDate() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df.format(new Date());
	}

	/*
	 * public void sendMail(String mailBody, String subject) throws IOException
	 * { Email email = new Email(null); email.setFrom(from);
	 * email.setSmtp(smtpHost); email.setBody(mailBody); email.setTo(to);
	 * email.setPassword(pwd); email.setEmailPort(portNumber);
	 * email.setSubject(subject); email.setNick(nick + " Monitor");
	 * SocketEmail.sendEmail(email); }
	 */

	public static void does(AmazonS3 asc, String bucket, String object)
			throws Exception {
		inition(asc, bucket, object);
		objectGet(asc, bucket, object);
		unInition(asc, bucket, object);
	}

	static class TaskSH extends TimerTask {
		private static long lastSendAlive = System.currentTimeMillis();
		private static long lastSendAlert = 0;
		private static boolean normalFlag = true;
		private static long oneDay = 86400000;
		private static boolean isSendAbnormal = false;
		private static final int TOTAL = 100;
		private static final int BUCKET_NUM = 10;
		private static Executor pool = Executors.newFixedThreadPool(BUCKET_NUM);
		private static DateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		
		private static String bucket = "bucket-name-for-oos-sh-monitor-7845589720135488896";
		private static String objectName = "object-name-for-oos-sh-monitor-6463084869102846986";
		@Override
		public void run() {
			ClientConfiguration configuration = new ClientConfiguration();
			configuration.setConnectionTimeout(30000);
			configuration.setSocketTimeout(30000);
			configuration.setMaxErrorRetry(0);
			AmazonS3 asc = null;
			try {
				asc = new AmazonS3Client(new AWSCredentials() {
					public String getAWSAccessKeyId() {
						return username;
					}

					public String getAWSSecretKey() {
						return password;
					}
				}, configuration);
				asc.setEndpoint(url);
			} catch (Exception e) {
				log.error(nick + "::" + e.getMessage(), e);
				throw new Error(e);
			}
			boolean flag = true;
			final AtomicInteger count = new AtomicInteger(0);
			final AtomicInteger bdCount = new AtomicInteger(0);
			long startTime = System.currentTimeMillis();
			// StringBuilder body = new StringBuilder();
			String body = null;
			String subject = null;
			/*try {
				String object = objectName + "_" + ThreadLocalRandom.current().nextInt(10000);
				does(asc, bucket, object);
				log.info(nick + " Try first time success!");
			} catch (Exception e1) {
				log.info(nick + " Try first time error!");
				if (!checkNetstat())
					bdCount.incrementAndGet();
				count.incrementAndGet();
				lastStatus = false;

			}
			if (!lastStatus) {*/
				final AtomicInteger num = new AtomicInteger(1);
				final AtomicInteger n = new AtomicInteger(0);
				final AmazonS3 client = asc;
				for (int i = 0; i < TOTAL; i++) {
					pool.execute(new Thread() {
						public void run() {
							int j = num.getAndIncrement();
							try {
							    int mn = ThreadLocalRandom.current().nextInt(10000);
								String object = objectName + "_" + mn + "_" + System.currentTimeMillis();
								does(client, bucket, object);
								log.info(nick + " Try " + j + " time success!");
							} catch (Throwable e2) {
								log.info(nick + " Try " + j + " time error:");
								log.error(nick + "::" + e2.getMessage(), e2);
								if (!checkNetstat())
									bdCount.incrementAndGet();
								count.incrementAndGet();
							}
							n.incrementAndGet();
						}
					});
				
				while (n.get() < TOTAL)
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        ;
                    }
				long endTime = System.currentTimeMillis();
				long time = (endTime - startTime) / 1000;
				if (count.get() > TOTAL * 40 / 100) {
					subject = "Status: Fatal";
					body = buildBody(0, nick, time, TOTAL, count.get(),
							bdCount.get());
					normalFlag = false;
					flag = false;
				} else if (count.get() > TOTAL * 20 / 100) {
					subject = "Status: Error";
					body = buildBody(1, nick, time, TOTAL, count.get(),
							bdCount.get());
					normalFlag = false;
					flag = false;
				} else if (count.get() > TOTAL * 10 / 100) {
					subject = "Status: Warning";
					body = buildBody(2, nick, time, TOTAL, count.get(),
							bdCount.get());
					normalFlag = false;
					flag = false;
				} else {
					subject = "Status: OK";
					body = buildBody(3, nick, time, TOTAL, count.get(),
							bdCount.get());
				}
			}

			if (!normalFlag) {
			    if (flag) {
                    if (isSendAbnormal) {
                        MailUtils.sendMail(subject, nick, body);
                        isSendAbnormal = false;
                    }
                    normalFlag = true;
                } else if (System.currentTimeMillis() - lastSendAlert >= sendEmailInter) {
					MailUtils.sendMail(subject, nick, body);
					lastSendAlert = System.currentTimeMillis();
					lastSendAlive = System.currentTimeMillis();
					isSendAbnormal = true;
				}

			} else {
				if (System.currentTimeMillis() - lastSendAlive >= oneDay) {
					subject = "Status: OK";
					body = buildBody(4, nick, 0, 0, 0, 0);
					MailUtils.sendMail(subject, nick, body);
					lastSendAlive = System.currentTimeMillis();
				}
			}

		}

		private static String buildBody(int type, String nick, long time,
				int total, int failure, int bdFailure) {
			StringBuilder sb = new StringBuilder(
					"<table width=\"358\" border=\"0\" cellspacing=\"5\" style=\"font-size:14px\">")
					.append("<tr>")
					.append("<td width=\"225\" align=\"left\" valign=\"middle\">监控源 ：</td>")
					.append("<td width=\"137\" align=\"left\" valign=\"middle\">");
			if (type == 0) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>")
						.append("<td>报警级别：</td>")
						.append("<td bgcolor=\"#FFFF33\"><strong style=\"color:#C63\">致命</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>超过百分之八十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 1) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>")
						.append("<td>报警级别：</td>")
						.append("<td bgcolor=\"#FFFF33\"><strong>严重</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>超过百分之五十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 2) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>")
						.append("<td>报警级别：</td>")
						.append("<td bgcolor=\"#FFFF33\"><strong>警告</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>超过百分之二十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 3) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>报警级别：</td>")
						.append("<td><strong>恢复正常</strong></td>")
						.append("</tr>");
				sb.append("<tr>").append("<td>失败率：</td>")
						.append("<td>不超过百分之二十</td>").append("</tr>");
				sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
						.append(time).append("s</td>").append("</tr>");
				sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
						.append(total).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
						.append(failure).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
						.append("<td>").append(bdFailure).append("</td>")
						.append("</tr>");
			} else if (type == 4) {
				sb.append(nick).append("</td>").append("</tr>");
				sb.append("<tr>").append("<td>报警级别：</td>")
						.append("<td><strong>正常</strong></td>").append("</tr>");
				sb.append("<tr>").append("<td>持续时间：</td>")
						.append("<td>过去的24小时</td>").append("</tr>");
			}
			sb.append("<tr>").append("<td>邮件发送时间：</td>").append("<td>")
					.append(format.format(new Date())).append("</td>")
					.append("</tr>");

			sb.append("</table>");
			sb.append("<p><em>注：</em></p>");
			sb.append(
					"<table width=\"653\" height=\"284\" border=\"1\" style=\"font-size:13px\">")
					.append("<tr>")
					.append("<td width=\"80\" rowspan=\"6\">报警级别 </td>")
					.append("<td width=\"563\">正常/恢复正常 &lt; 警告 &lt; 严重 &lt; 致命</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>正常（在过去的24小时中，OOS的状态都是正常的）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>恢复正常（在上次监控的失败率 &gt; 20%情况下，本次监控的失败率 &lt; 20%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>警告（失败率 &gt; 20%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>严重（失败率 &gt; 50%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>致命（失败率 &gt; 80%）</td>")
					.append("</tr>")
					.append("<tr>")
					.append("<td>监控原则</td>")
					.append("<td><p>&nbsp;&nbsp;&nbsp;&nbsp;监控程序10秒运行一次，每次监控都访问100次OOS服务，每次服务包括put/get/delete object，然后进行统计分析，并根据不同的失败率发送不同级别的报警邮件；每次发送报警邮件之前都会判断上次发送报警邮件的时间是否已经超过十分钟，如果超过则发送报警邮件，否则不发送；如果监控程序已经超过24小时未出现失败率 &lt; 10%的情况，则会发送一封正常邮件。</p><p>&nbsp;&nbsp;&nbsp;&nbsp;在发送警告/严重/致命邮件之前，都会判断上次发送此类级别邮件的时间是否已经超过10分钟，只有时间间隔超过10分钟，才会再次发送此类级别的报警邮件。</p></td>")
					.append("</tr>").append("</table>");

			return sb.toString();
		}

	}
	
	static class TaskSHX extends TimerTask {
        private static long lastSendAlive = System.currentTimeMillis();
        private static long lastSendAlert = 0;
        private static boolean normalFlag = true;
        private static long oneDay = 86400000;
        private static boolean isSendAbnormal = false;
        private static final int TOTAL = 100;
        private static final int BUCKET_NUM = 10;
        private static Executor pool = Executors.newFixedThreadPool(BUCKET_NUM);
        private static DateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");
        
        private static String bucket = "bucket-name-for-oos-shx-monitor-7845589720135488896";
        private static String objectName = "object-name-for-oos-shx-monitor-6463084869102846986";
        @Override
        public void run() {
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setConnectionTimeout(10000);
            configuration.setSocketTimeout(10000);
            configuration.setMaxErrorRetry(0);
            AmazonS3 asc = null;
            try {
                asc = new AmazonS3Client(new AWSCredentials() {
                    public String getAWSAccessKeyId() {
                        return username;
                    }

                    public String getAWSSecretKey() {
                        return password;
                    }
                }, configuration);
                asc.setEndpoint(url);
            } catch (Exception e) {
                log.error(nick + "::" + e.getMessage(), e);
                throw new Error(e);
            }
            boolean flag = true;
            final AtomicInteger count = new AtomicInteger(0);
            final AtomicInteger bdCount = new AtomicInteger(0);
            long startTime = System.currentTimeMillis();
            // StringBuilder body = new StringBuilder();
            String body = null;
            String subject = null;
            /*try {
                String object = objectName + "_" + ThreadLocalRandom.current().nextInt(10000);
                does(asc, bucket, object);
                log.info(nick + " Try first time success!");
            } catch (Exception e1) {
                log.info(nick + " Try first time error!");
                if (!checkNetstat())
                    bdCount.incrementAndGet();
                count.incrementAndGet();
                lastStatus = false;

            }
            if (!lastStatus) {*/
                final AtomicInteger num = new AtomicInteger(1);
                final AtomicInteger n = new AtomicInteger(1);
                final AmazonS3 client = asc;
                for (int i = 1; i < TOTAL; i++) {
                    pool.execute(new Thread() {
                        public void run() {
                            int j = num.getAndIncrement();
                            try {
                                int mn = ThreadLocalRandom.current().nextInt(10000);
                                String object = objectName + "_" + mn + "_" + System.currentTimeMillis();
                                does(client, bucket, object);
                                log.info(nick + " Try " + j + " time success!");
                            } catch (Exception e2) {
                                log.info(nick + " Try " + j + " time error:");
                                log.error(nick + "::" + e2.getMessage(), e2);
                                if (!checkNetstat())
                                    bdCount.incrementAndGet();
                                count.incrementAndGet();
                            }
                            n.incrementAndGet();
                        }
                    });
                
                while (n.get() < TOTAL)
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        ;
                    }
                long endTime = System.currentTimeMillis();
                long time = (endTime - startTime) / 1000;
                if (count.get() > TOTAL * 40 / 100) {
                    subject = "Status: Fatal";
                    body = buildBody(0, nick, time, TOTAL, count.get(),
                            bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > TOTAL * 20 / 100) {
                    subject = "Status: Error";
                    body = buildBody(1, nick, time, TOTAL, count.get(),
                            bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else if (count.get() > TOTAL * 10 / 100) {
                    subject = "Status: Warning";
                    body = buildBody(2, nick, time, TOTAL, count.get(),
                            bdCount.get());
                    normalFlag = false;
                    flag = false;
                } else {
                    subject = "Status: OK";
                    body = buildBody(3, nick, time, TOTAL, count.get(),
                            bdCount.get());
                }
            }

            if (!normalFlag) {
                if (flag) {
                    if (isSendAbnormal) {
                        MailUtils.sendMail(subject, nick, body);
                        isSendAbnormal = false;
                    }
                    normalFlag = true;
                } else if (System.currentTimeMillis() - lastSendAlert >= sendEmailInter) {
                    MailUtils.sendMail(subject, nick, body);
                    lastSendAlert = System.currentTimeMillis();
                    lastSendAlive = System.currentTimeMillis();
                    isSendAbnormal = true;
                }

            } else {
                if (System.currentTimeMillis() - lastSendAlive >= oneDay) {
                    subject = "Status: OK";
                    body = buildBody(4, nick, 0, 0, 0, 0);
                    MailUtils.sendMail(subject, nick, body);
                    lastSendAlive = System.currentTimeMillis();
                }
            }

        }

        private static String buildBody(int type, String nick, long time,
                int total, int failure, int bdFailure) {
            StringBuilder sb = new StringBuilder(
                    "<table width=\"358\" border=\"0\" cellspacing=\"5\" style=\"font-size:14px\">")
                    .append("<tr>")
                    .append("<td width=\"225\" align=\"left\" valign=\"middle\">监控源 ：</td>")
                    .append("<td width=\"137\" align=\"left\" valign=\"middle\">");
            if (type == 0) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong style=\"color:#C63\">致命</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之八十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 1) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong>严重</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之五十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 2) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong>警告</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之二十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 3) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>报警级别：</td>")
                        .append("<td><strong>恢复正常</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>不超过百分之二十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 4) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>报警级别：</td>")
                        .append("<td><strong>正常</strong></td>").append("</tr>");
                sb.append("<tr>").append("<td>持续时间：</td>")
                        .append("<td>过去的24小时</td>").append("</tr>");
            }
            sb.append("<tr>").append("<td>邮件发送时间：</td>").append("<td>")
                    .append(format.format(new Date())).append("</td>")
                    .append("</tr>");

            sb.append("</table>");
            sb.append("<p><em>注：</em></p>");
            sb.append(
                    "<table width=\"653\" height=\"284\" border=\"1\" style=\"font-size:13px\">")
                    .append("<tr>")
                    .append("<td width=\"80\" rowspan=\"6\">报警级别 </td>")
                    .append("<td width=\"563\">正常/恢复正常 &lt; 警告 &lt; 严重 &lt; 致命</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>正常（在过去的24小时中，OOS的状态都是正常的）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>恢复正常（在上次监控的失败率 &gt; 20%情况下，本次监控的失败率 &lt; 20%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>警告（失败率 &gt; 20%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>严重（失败率 &gt; 50%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>致命（失败率 &gt; 80%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>监控原则</td>")
                    .append("<td><p>&nbsp;&nbsp;&nbsp;&nbsp;监控程序10秒运行一次，每次监控都访问100次OOS服务，每次服务包括put/get/delete object，然后进行统计分析，并根据不同的失败率发送不同级别的报警邮件；每次发送报警邮件之前都会判断上次发送报警邮件的时间是否已经超过十分钟，如果超过则发送报警邮件，否则不发送；如果监控程序已经超过24小时未出现失败率 &lt; 10%的情况，则会发送一封正常邮件。</p><p>&nbsp;&nbsp;&nbsp;&nbsp;在发送警告/严重/致命邮件之前，都会判断上次发送此类级别邮件的时间是否已经超过10分钟，只有时间间隔超过10分钟，才会再次发送此类级别的报警邮件。</p></td>")
                    .append("</tr>").append("</table>");

            return sb.toString();
        }

    }
	
	static class TaskSH2 extends TimerTask {
        private static long lastSendAlive = System.currentTimeMillis();
        
        private static long oneDay = 86400000;
        private static final int TOTAL = 15;
        private static final int BUCKET_NUM = 10;
        private static Executor pool = Executors
                .newFixedThreadPool(BUCKET_NUM * 2);
        private static DateFormat format = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss");

        private final static Map<String, Boolean> LAST_SH2 = new ConcurrentHashMap<String, Boolean>();
        private final static Map<String, Boolean> SEND_ABNORMAL_SH2 = new ConcurrentHashMap<String, Boolean>();
        private final static Map<String, Boolean> NORMAL_FLAG_SH2 = new ConcurrentHashMap<String, Boolean>();
        private final static Map<String, Long> LAST_SEND_ALERT = new ConcurrentHashMap<String, Long>();

        private static Object lock = new Object();

        static {
            Set<String> keys = objectNames.keySet();
            Iterator<String> itr = keys.iterator();
            while (itr.hasNext()) {
                String key = itr.next();
                LAST_SH2.put(key, true);
                SEND_ABNORMAL_SH2.put(key, false);
                NORMAL_FLAG_SH2.put(key, true);
                LAST_SEND_ALERT.put(key, System.currentTimeMillis());
            }
        }

        @Override
        public void run() {
            ClientConfiguration configuration = new ClientConfiguration();
            /*configuration.setConnectionTimeout(10000);
            configuration.setSocketTimeout(10000);*/
            configuration.setMaxErrorRetry(0);
            AmazonS3 asc = null;
            try {
                asc = new AmazonS3Client(new AWSCredentials() {
                    public String getAWSAccessKeyId() {
                        return username;
                    }

                    public String getAWSSecretKey() {
                        return password;
                    }
                }, configuration);
                asc.setEndpoint(url);
            } catch (Exception e) {
                log.error(nick + "::" + e.getMessage(), e);
                throw new Error(e);
            }
            // StringBuilder body = new StringBuilder();
            Set<String> keys = objectNames.keySet();
            Iterator<String> itr = keys.iterator();
            final Set<Integer> buckets = Collections
                    .synchronizedSet(new HashSet<Integer>());
            final AmazonS3 client = asc;
            final AtomicInteger complete = new AtomicInteger(0);
            while (itr.hasNext()) {
                final String key = itr.next();
                final String value = objectNames.get(key);
                final String object = objectName + "_" + key;

                new Thread() {
                    public void run() {
                        final AtomicInteger count = new AtomicInteger(0);
                        final AtomicInteger bdCount = new AtomicInteger(0);
                        long startTime = System.currentTimeMillis();
                        boolean flag = true;
                        String body = null;
                        String subject = null;
                        int j = 0;
                        int m = j % BUCKET_NUM;
                        synchronized (lock) {
                            while (buckets.contains(m)) {
                                m = ++m % BUCKET_NUM;
                                m = m < 0 ? 0 : m;
                            }
                            buckets.add(m);
                        }
                        try {
                            String bucket = bucketName + m;
                            does(client, bucket, object);
                            log.info(nick + "_" + value
                                    + " Try first time success!");
                        } catch (Exception e1) {
                            log.info(nick + "_" + value
                                    + " Try first time error!");
                            log.error(e1.getMessage(), e1);
                            if (!checkNetstat())
                                bdCount.incrementAndGet();
                            count.incrementAndGet();
                            LAST_SH2.put(key, false);

                        }
                        buckets.remove(m);
                        if (!LAST_SH2.get(key)) {
                            final AtomicInteger n = new AtomicInteger(1);
                            final AtomicInteger num = new AtomicInteger(1);
                            for (int i = 1; i < TOTAL; i++) {
                                pool.execute(new Thread() {
                                    public void run() {
                                        int j = num.getAndIncrement();
                                        int m = j % BUCKET_NUM;
                                        synchronized (lock) {
                                            while (buckets.contains(m)) {
                                                m = ++m % BUCKET_NUM;
                                                m = m < 0 ? 0 : m;
                                            }
                                            buckets.add(m);
                                        }
                                        try {
                                            String bucket = bucketName + m;
                                            does(client, bucket, object);
                                            log.info(nick + "_" + value
                                                    + " Try " + j
                                                    + " time success!");
                                        } catch(Exception e2) {
                                            log.info(nick + "_" + value
                                                    + " Try " + j
                                                    + " time error:");
                                            log.error(nick + "_" + value + "::"
                                                    + e2.getMessage(), e2);
                                            if (!checkNetstat())
                                                bdCount.incrementAndGet();
                                            count.incrementAndGet();
                                        }
                                        buckets.remove(m);
                                        n.incrementAndGet();
                                    }
                                });
                            }
                            while (n.get() < TOTAL)
                                Thread.yield();
                            long endTime = System.currentTimeMillis();
                            long time = (endTime - startTime) / 1000;
                            if (count.get() > TOTAL * 80 / 100) {
                                subject = "Status: Fatal";
                                body = buildBody(value, 0, nick, time, TOTAL,
                                        count.get(), bdCount.get());
                                NORMAL_FLAG_SH2.put(key, false);
                                flag = false;
                            } else if (count.get() > TOTAL * 50 / 100) {
                                subject = "Status: Error";
                                body = buildBody(value, 1, nick, time, TOTAL,
                                        count.get(), bdCount.get());
                                NORMAL_FLAG_SH2.put(key, false);
                                flag = false;
                            } else if (count.get() > TOTAL * 20 / 100) {
                                subject = "Status: Warning";
                                body = buildBody(value, 2, nick, time, TOTAL,
                                        count.get(), bdCount.get());
                                NORMAL_FLAG_SH2.put(key, false);
                                flag = false;
                            } else {
                                subject = "Status: OK";
                                body = buildBody(value, 3, nick, time, TOTAL,
                                        count.get(), bdCount.get());
                                LAST_SH2.put(key, true);
                            }
                        }

                        if (!NORMAL_FLAG_SH2.get(key)) {
                            lastSendAlive = System.currentTimeMillis();
                            if (System.currentTimeMillis()
                                    - LAST_SEND_ALERT.get(key) >= sendEmailInter) {
                                MailUtils.sendMail(subject, nick + " Monitor",
                                        body);
                                LAST_SEND_ALERT.put(key,
                                        System.currentTimeMillis());
                                SEND_ABNORMAL_SH2.put(key, true);
                            }
                            if (flag) {
                                if (SEND_ABNORMAL_SH2.get(key)) {
                                    MailUtils.sendMail(subject, nick
                                            + " Monitor", body);
                                    NORMAL_FLAG_SH2.put(key, true);
                                    SEND_ABNORMAL_SH2.put(key, false);
                                }
                                NORMAL_FLAG_SH2.put(key, true);
                            }

                        } 
                        complete.getAndIncrement();
                    }
                }.start();
            }
            while (complete.get() < keys.size()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    ;
                }
            }
            if (System.currentTimeMillis() - lastSendAlive >= oneDay) {
                String subject = "Status: OK";
                String body = buildBody(null, 4, nick, 0, 0, 0, 0);
                MailUtils.sendMail(subject, nick + " Monitor",
                        body);
                lastSendAlive = System.currentTimeMillis();
            }

        }

        private static String buildBody(String num, int type, String nick,
                long time, int total, int failure, int bdFailure) {
            StringBuilder sb = new StringBuilder(
                    "<table width=\"358\" border=\"0\" cellspacing=\"5\" style=\"font-size:14px\">")
                    .append("<tr>")
                    .append("<td width=\"225\" align=\"left\" valign=\"middle\">监控源 ：</td>")
                    .append("<td width=\"137\" align=\"left\" valign=\"middle\">");
            if (type == 0) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
                        .append(num).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong style=\"color:#C63\">致命</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之八十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 1) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
                        .append(num).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong>严重</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之五十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 2) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
                        .append(num).append("</td>").append("</tr>");
                sb.append("<tr>")
                        .append("<td>报警级别：</td>")
                        .append("<td bgcolor=\"#FFFF33\"><strong>警告</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>超过百分之二十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 3) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>目标数据库：</td>").append("<td>")
                        .append(num).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>报警级别：</td>")
                        .append("<td><strong>恢复正常</strong></td>")
                        .append("</tr>");
                sb.append("<tr>").append("<td>失败率：</td>")
                        .append("<td>不超过百分之二十</td>").append("</tr>");
                sb.append("<tr>").append("<td>耗时：</td>").append("<td>")
                        .append(time).append("s</td>").append("</tr>");
                sb.append("<tr>").append("<td>尝试次数：</td>").append("<td>")
                        .append(total).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>失败次数：</td>").append("<td>")
                        .append(failure).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>测试机本身网络问题导致的失败次数：</td>")
                        .append("<td>").append(bdFailure).append("</td>")
                        .append("</tr>");
            } else if (type == 4) {
                sb.append(nick).append("</td>").append("</tr>");
                sb.append("<tr>").append("<td>报警级别：</td>")
                        .append("<td><strong>正常</strong></td>").append("</tr>");
                sb.append("<tr>").append("<td>持续时间：</td>")
                        .append("<td>过去的24小时</td>").append("</tr>");
            }
            sb.append("<tr>").append("<td>邮件发送时间：</td>").append("<td>")
                    .append(format.format(new Date())).append("</td>")
                    .append("</tr>");
            sb.append("<tr>").append("<td>监控数据库与所在机器对应关系：</td>").append("<td>");
            for(String ss : oosDatabase)
                sb.append(ss).append("<br/>");
            sb.append("</td>").append("</tr>");
            
            sb.append("</table>");
            sb.append("<p><em>注：</em></p>");
            sb.append(
                    "<table width=\"653\" height=\"284\" border=\"1\" style=\"font-size:13px\">")
                    .append("<tr>")
                    .append("<td width=\"80\" rowspan=\"6\">报警级别 </td>")
                    .append("<td width=\"563\">正常/恢复正常 &lt; 警告 &lt; 严重 &lt; 致命</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>正常（在过去的24小时中，OOS的状态都是正常的）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>恢复正常（在上次监控的失败率 &gt; 20%情况下，本次监控的失败率 &lt; 20%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>警告（失败率 &gt; 20%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>严重（失败率 &gt; 50%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>致命（失败率 &gt; 80%）</td>")
                    .append("</tr>")
                    .append("<tr>")
                    .append("<td>监控原则</td>")
                    .append("<td><p>&nbsp;&nbsp;&nbsp;&nbsp;监控程序每30s运行一次，每次监控都至少访问30次OOS服务，使得一次OOS服务能对应到一台目标服务器，如果第一次访问OOS服务时出现异常，监控程序会在较短的时间内并发的访问29次OOS，然后进行统计分析，并根据不同的失败率发送不同级别的报警邮件；如果上次运行监控程序时出现失败率 &gt; 20%的情况，则在本次监控中会无条件的在较短的时间内并发的访问14次OOS，然后进行统计分析，如果失败率 &lt; 20%，则会发生恢复正常邮件，否则判断上次发送报警邮件的时间是否已经超过十分钟，如果超过则继续发送报警邮件，否则不发送；如果第一次访问OOS服务成功，监控程序等待下一次运行；如果监控程序已经超过24小时未出现失败率 &lt; 20%的情况，则会发送一封正常邮件。</p><p>&nbsp;&nbsp;&nbsp;&nbsp;在发送警告/严重/致命邮件之前，都会判断上次发送此类级别邮件的时间是否已经超过10分钟，只有时间间隔超过10分钟，才会再次发送此类级别的报警邮件。</p></td>")
                    .append("</tr>").append("</table>");

            return sb.toString();
        }

    }

	@Override
	public String usage() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void exec(String[] args) throws Exception {
		Timer timer = new Timer();
		if (nick.equals("OOS-CD"))
			timer.schedule(new TaskCD(), 0, runInter);
		else if (nick.equals("OOS-SH"))
			timer.schedule(new TaskSH(), 0, runInter);
		else
		    timer.schedule(new TaskSH(), 0, runInter);

	}
	
	public static void main(String[] args) {
	    int dbNum=30;
	    HashMap<Integer,Integer> res = new HashMap<Integer,Integer>();
	    for(int i=1;i<1000;i++){
	        String name = objectName + "_" + i; 
	        int num = Math.abs((int) (MD5Hash.digest(name).halfDigest() % dbNum));
	        if(!res.containsKey(num))
	            res.put(num, i);
	        if(res.size() == dbNum)
	            break;
	    }
	    Set<Integer> keys = res.keySet();
	    Iterator<Integer> itr = keys.iterator();
	    while(itr.hasNext()){
	        int i = itr.next();
	        System.out.print(i + ":" + res.get(i) + ",");
	    }
    }

}