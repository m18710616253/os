package cn.ctyun.oos.tempo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import common.util.MD5Hash;

public class OOSDBTest3 {

	private final String BUCKET_FILE_URL = "/ibdata/ibdata4/bucket.txt";
	private final String OWNER_FILE_URL = "/ibdata/ibdata4/owner.txt";
	private final String OBJECT_FILE_URL = "/ibdata/ibdata4/";

	private long bucketCount = 10;
	private long ownerCount = 100;

	public OOSDBTest3() {

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			new OOSDBTest3().exec(args);
			while(Thread.activeCount()>1)
				Thread.yield();
			System.out.println("done!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void exec(String[] args) {
		File ownerFile = new File(OWNER_FILE_URL);
		if (ownerFile.exists()) {
			ownerFile.delete();
		}

		File bucketFile = new File(BUCKET_FILE_URL);
		if (bucketFile.exists()) {
			bucketFile.delete();
		}
		FileWriter fw1 = null;
		try {
			fw1 = new FileWriter(OWNER_FILE_URL, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileWriter fw2 = null;
		try {
			fw2 = new FileWriter(BUCKET_FILE_URL, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			for (int i = 1; i <= ownerCount; i++) {
				String owner = createOwner(i);
				fw1.write(owner);

				// System.out.println("owner "+Integer.toString(ownerId));
				for (int j = 0; j < bucketCount; j++) {
					String bucketName = "bucket-" + i + "-" + j;
					long id = getBucketID(bucketName);
					String bucket = createBucket(id, bucketName, i, j);
					fw2.write(bucket);
					// System.out.println("bucket "+bukid);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (fw1 != null)
				try {
					fw1.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (fw2 != null)
				try {
					fw2.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		final int inter = 1000000;
		final AtomicInteger z = new AtomicInteger(0);
		final AtomicInteger objNum = new AtomicInteger(0);
		final AtomicInteger ownerNum = new AtomicInteger(0);
		for (int i = 0; i < 20; i++) {
			
			new Thread() {
				public void run() {
					String fileName = "/ibdata/ibdata4/" + "object"
							+ z.getAndIncrement() + ".txt";
					File objectFile = new File(fileName);
					if (objectFile.exists()) {
						objectFile.delete();
					}
					FileWriter fw3 = null;
					try {
						fw3 = new FileWriter(fileName, true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
							int o = ownerNum.incrementAndGet();
							for (int q = 0; q < bucketCount; q++) {
								int n = objNum.getAndIncrement();
								for (int k = n * inter; k < (n + 1) * inter; k++) {

									String objectName = "object" + "-" + k;
									String objid = getObjectID(objectName);
									String bucketName = "bucket-" + o + "-" + q;
									long id = getBucketID(bucketName);
									String bukid = Long.toString(id);
									String object = createObject(objid,
											objectName, bukid, k);
									fw3.write(object);
								}
							}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (fw3 != null)
							try {
								fw3.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}

				}
			}.start();
		}
		for (int i = 0; i < 20; i++) {
			
			new Thread() {
				public void run() {
					String fileName = "/ibdata/ibdata5/" + "object"
							+ z.getAndIncrement() + ".txt";
					File objectFile = new File(fileName);
					if (objectFile.exists()) {
						objectFile.delete();
					}
					FileWriter fw3 = null;
					try {
						fw3 = new FileWriter(fileName, true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
							int o = ownerNum.incrementAndGet();
							for (int q = 0; q < bucketCount; q++) {
								int n = objNum.getAndIncrement();
								for (int k = n * inter; k < (n + 1) * inter; k++) {

									String objectName = "object" + "-" + k;
									String objid = getObjectID(objectName);
									String bucketName = "bucket-" + o + "-" + q;
									long id = getBucketID(bucketName);
									String bukid = Long.toString(id);
									String object = createObject(objid,
											objectName, bukid, k);
									fw3.write(object);
								}
							}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (fw3 != null)
							try {
								fw3.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}

				}
			}.start();
		}
		for (int i = 0; i < 20; i++) {
			
			new Thread() {
				public void run() {
					String fileName = "/ibdata/ibdata6/" + "object"
							+ z.getAndIncrement() + ".txt";
					File objectFile = new File(fileName);
					if (objectFile.exists()) {
						objectFile.delete();
					}
					FileWriter fw3 = null;
					try {
						fw3 = new FileWriter(fileName, true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
							int o = ownerNum.incrementAndGet();
							for (int q = 0; q < bucketCount; q++) {
								int n = objNum.getAndIncrement();
								for (int k = n * inter; k < (n + 1) * inter; k++) {

									String objectName = "object" + "-" + k;
									String objid = getObjectID(objectName);
									String bucketName = "bucket-" + o + "-" + q;
									long id = getBucketID(bucketName);
									String bukid = Long.toString(id);
									String object = createObject(objid,
											objectName, bukid, k);
									fw3.write(object);
								}
							}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (fw3 != null)
							try {
								fw3.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}

				}
			}.start();
		}
		
	}

	public String createOwner(long num) {
		long mis = getCurrentDateLong() + num;
		StringBuilder sb = new StringBuilder();
		sb.append(num).append(",");
		sb.append("crury" + num).append(",");
		sb.append("cuimeng" + num).append(",");
		sb.append("01" + num).append(",");
		sb.append("crury" + num + "@qq.com").append(",");
		sb.append("ctyun" + num).append(",");
		sb.append("西山赢府" + num).append(",");
		sb.append("1891000" + num).append(",");
		sb.append("0102522" + num).append(",");
		sb.append("VEC" + num).append(",");
		sb.append("1" + num).append(",");
		sb.append(mis).append(",");
		sb.append("10" + num).append(",");
		sb.append(mis);
		sb.append("\n");
		return sb.toString();
	}

	public String createBucket(long bucketId, String bucketName,
			long ownerId, long id) {
		long mis = getCurrentDateLong();
		StringBuilder sb = new StringBuilder();
		sb.append(bucketId).append(",");
		sb.append(bucketName).append(",");
		sb.append(ownerId).append(",");
		sb.append("china").append(",");
		sb.append("private").append(",");
		sb.append(mis).append(",");
		sb.append("<xml>" + id + "</xml>").append(",");
		sb.append("index" + id + ".html").append(",");
		sb.append("error" + id + ".html");
		sb.append("\n");
		return sb.toString();
	}

	public String createObject(String objectId, String objectName,
			String bucketId, long id) {
		long mis = getCurrentDateLong();
		StringBuilder sb = new StringBuilder();
		sb.append(objectId).append(",");
		sb.append(objectName).append(",");
		sb.append(bucketId).append(",");
		sb.append("200" + id).append(",");
		sb.append("ajlkfdjk" + id).append(",");
		sb.append(mis).append(",");
		sb.append("meta" + id).append(",");
		sb.append("cacheControl`" + id).append(",");
		sb.append("contentEncoding" + id).append(",");
		sb.append("contentMD5" + id).append(",");
		sb.append("text/plain").append(",");
		sb.append("contentDisposition" + id).append(",");
		sb.append(id);
		sb.append("\n");
		return sb.toString();
	}

	public long getBucketID(String name) {
		return MD5Hash.digest(name).halfDigest();
	}

	public String getObjectID(String name) {
		return MD5Hash.digest(name).toString();
	}

	public String getCurrentDateString() {
		return Long.toString(getCurrentDateLong());
	}

	public long getCurrentDateLong() {
		return System.currentTimeMillis();
	}

}
