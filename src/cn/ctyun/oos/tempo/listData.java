package cn.ctyun.oos.tempo;


import java.io.File;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.channels.FileChannel;

import java.util.concurrent.atomic.AtomicLong;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.ostor.utils.CRCUtils;
//import cn.ctyun.oos.tempo.PutGetTest2;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;


import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import common.io.StreamUtils;
import common.threadlocal.ThreadLocalBytes;
import common.util.BlockingExecutor;

public class listData {
	private static Log log = LogFactory.getLog(listData.class);
	public static String mountPath = "d:/s3";
	public static AtomicLong totalBytes = new AtomicLong();
	static AmazonS3Client client1;
	static AmazonS3Client client2;
	
	static {
        System.setProperty("log4j.log.app", "move");
    }
	private static ClientConfiguration cc = new ClientConfiguration();

	static {
	        cc.setConnectionTimeout(60000*3);
	        cc.setSocketTimeout(60000*3);
	 }

	public static void main(String[] args) throws Exception {

		String DOMAIN2 = "http://oos-nm.ctyunapi.cn";
		final String ak2 = "c1a3f491280f20de1016";
		final String sk2 = "8fedd5af3485def7966bda90e2a08c878154b3db";
	    String bucketname = null;
	    
	    String DOMAIN1 = "http://oos-nm2.ctyunapi.cn";
		final String ak1 = "95fad2827c920d75953d";
		final String sk1 = "6837fe73aecd4795391aedd42e2889cfd8070db5";
			
        final AtomicLong total = new AtomicLong();
        final AtomicLong readSuc = new AtomicLong();
        final AtomicLong putSuc = new AtomicLong();
             
        Thread t = new Thread() {
            @Override
            public void run() {
                for (;;) {
                	log.info("totalBytes = " + totalBytes.get());
                    log.info("Total = " + total.get());
                    log.info("readSuc = " + readSuc.get());
                    log.info("putSuc = " + putSuc.get());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();

    	client1 = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return ak1;
            }
            
            public String getAWSSecretKey() {
                return sk1;
            }
        }, cc);
   
        client2 = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return ak2;
            }
            
            public String getAWSSecretKey() {
                return sk2;
            }
        }, cc);
        client1.setEndpoint(DOMAIN1);
        client2.setEndpoint(DOMAIN2);         

		BlockingExecutor executor = new BlockingExecutor(1000, 1000, 1000, 3000, "Thread");
		log.info("bucket size: "+client1.listBuckets().size());
		for(Bucket bucket : client1.listBuckets()){
			log.info("bucket name: "+ bucket.getName());
			try{
				bucketname = bucket.getName();
				client2.createBucket(bucketname);
			}catch(AmazonS3Exception e){
				if(e.getStatusCode() == 409){
					File File1 = new File(listData.mountPath, "未迁移的bucket名字");
					FileOutputStream txtfile = null;
					txtfile = new FileOutputStream(File1);
					PrintStream p = new PrintStream(txtfile);
				   	p.println("\n表中未迁移的bucket名字，原因为内蒙2中已存在该bucket名字\n");
				   	p.println(bucketname + "\n");
				   	if (p != null)
				     p.close();
					continue;
				}
			}
			
			ListObjectsRequest request = new ListObjectsRequest();
	        request.setBucketName(bucketname);
	        request.setMaxKeys(100);
	        ObjectListing os1 = client1.listObjects(request);  
	        int size = 0;
	        File docFile = new File(listData.mountPath, "未迁移成功的bucket及objectname");
	        String srl = null;
	       
	        do{
	        	for (S3ObjectSummary o : os1.getObjectSummaries()) { 
	        		 S3Object object =  client1.getObject(bucketname, o.getKey());   
		        	 String key = o.getKey();
		        	 String sts = o.getStorageClass();
		        	 long objsize = o.getSize();
		             try{
		            	 executor.execute(new MovThread(bucketname, key, sts, objsize, total, readSuc,
		            		 putSuc, object, client2));
                     }catch(Throwable e){
                    	 FileOutputStream txtfile = null;
					 try {
						 txtfile = new FileOutputStream(docFile);
					 } catch (FileNotFoundException e1) {
						 e1.printStackTrace();
					 }
					 	PrintStream p = new PrintStream(txtfile);
					 	p.println("bucket" + ":" + bucketname +";" +"objectname"+ ":" + key +"\n");
                     }
	        	}
//	            log.info(os1.getNextMarker() + " " + os1.isTruncated());
	        	if (!os1.isTruncated())
		            break;
		         request.setMarker(os1.getNextMarker());
		         os1 = client1.listObjects(request);
		         size = os1.getObjectSummaries().size();  
	        }while(size !=0 );
	        
//	        if(docFile.length()!= 0){
//	        	try{
//		        	FileInputStream fstream = new FileInputStream(docFile);
//		        	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
//		        	while((srl= br.readLine()) != null){
//			        	String key = srl.split(":")[1];
//			        	S3Object object =  client1.getObject(bucketname, key);   
//			        	String sts = object.getObjectMetadata().getUserMetadata().get("storage");
//			        	long objsize = object.getObjectMetadata().getUserMetadata().size();
//	                    executor.execute(new MovThread(bucketname, key, sts, objsize, total, readSuc,
//			        				putSuc, object, client2));
//			        }
//			        br.close();
//		        }catch(Throwable e){
//		        	log.error("move second error:" + "bucketname:" + bucketname + " ", e);
//		        }
//	        }
	        
	        while (true) {
	            if (executor.getActiveCount() > 0)
	                Thread.sleep(100);
	            else
	                break;
	        }
		}
	} 
}
   

class MovThread implements Runnable {
    AtomicLong total;
    AtomicLong getSuc;
    AtomicLong putSuc;
    long length;
    AmazonS3Client client2;
    S3Object object;
    String bucketName;
    private static final Log log = LogFactory.getLog(MovThread.class);
    String sts;
    String key;
    boolean GetSuc = true;
    boolean PutSuc = true;
    long size;
    
    public  boolean getChecksum( File file, String sts, long length, S3Object object, 
    		String key) throws IOException {
        boolean checkSuc = false; 
        FileInputStream fis = new FileInputStream(file);  
        FileChannel fc =  fis.getChannel(); 
        if ((file.length() == length) && (size == fc.size())) {
            checkSuc = true;
            getSuc.getAndIncrement();
            log.info("download success " + key + " length:"
                        + length + " storage:"
                        + sts);
        } else{
        	
        	log.error("download fail " + key + " length:"
	                    + object.getObjectMetadata().getContentLength() + " meta storage:"
	                    + object.getObjectMetadata().getUserMetadata().get("storage")
	                    + " real storage:"
	                    + sts);
	        checkSuc = false;
        } 
        if (fis != null)
        	fis.close();
        return checkSuc;
   }  
    public  boolean putChecksum( File file, String sts, long length, S3Object object, 
    		String key) throws IOException {
        boolean checkSuc = false; 
        S3ObjectInputStream input = object.getObjectContent();   
        long crc1 = Long.parseLong(object.getObjectMetadata().getUserMetadata()
                     .get("crc"));;
        long crc2 = CRCUtils.getCRC32Checksum(file);
        byte[] bytes = ThreadLocalBytes.current().get1KBytes();
        long get = 0;
        while(get < size) {
              long len = input.read(bytes);
              get += len;
        }             
        assert(size == get);
        listData.totalBytes.getAndAdd(size);
        if ((crc1 == crc2)  && (file.length() == length)) {
            checkSuc = true;
            log.info("put success " + key + " length:"
                        + object.getObjectMetadata().getContentLength() + " storage:"
                        + sts);
        } else{
        	log.error("put fail " + key + " length:"
	                    + object.getObjectMetadata().getContentLength() + " meta storage:"
	                    + object.getObjectMetadata().getUserMetadata().get("storage")
	                    + " real storage:"
	                    + sts + " crc1:"
	                    + crc1 + " crc2:" + crc2);
        	checkSuc = false;
        } 
        return checkSuc;
   }  
    
    public MovThread(String bucketname, String key, String sts, long objksize, AtomicLong total, 
    		AtomicLong GetSuc, AtomicLong putSuc, S3Object object, AmazonS3Client client2) {
        this.total = total;
        this.putSuc = putSuc;
        this.length = object.getObjectMetadata().getContentLength();
        this.getSuc = GetSuc;
        this.client2 = client2;
        this.bucketName = bucketname;
        this.sts = sts;
        this.key = key;
        this.object = object;
        this.size = objksize;
    }
    
    @Override
    public void run() {
    	File file = new File(listData.mountPath, key);
        try {
            S3ObjectInputStream input = object.getObjectContent();       
            FileOutputStream fos = new FileOutputStream(file); 
            StreamUtils.copy(input, fos, length);
            if(getChecksum(file, sts, length, object, key)){
            	try{
	            		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, file);
	                    ObjectMetadata metadata = new ObjectMetadata();   
	                    metadata.addUserMetadata("storage", sts);
	                    putObjectRequest.setStorageClass(sts);    
	                    metadata.addUserMetadata("crc",  String.valueOf(CRCUtils.getCRC32Checksum(file)));
	                    putObjectRequest.setMetadata(metadata);
	                    client2.putObject(putObjectRequest);
	                    S3Object object2 = client2.getObject(bucketName, key);
	                    if(putChecksum(file, sts, length, object2, key)){
	                    	 putSuc.getAndIncrement();
	                    	 log.info("move success:" + key + " length:" + file.length() + " "); 
	                    }
            	} catch (Exception e) {
                     log.error("put error:" + key + " length:" + file.length() + " ", e);
                }	
            } 
            try {
                if (fos != null)
                     fos.close();
            } finally {
                if (input != null)
                     input.close();
            }
        } catch (Exception e) {
             log.error("move error:" + key + " length:" + file.length() + " ", e);
            
        } finally {
            file.delete();
            total.getAndIncrement();
        }
    }

  }



