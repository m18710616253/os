package cn.ctyun.oos.tempo;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import common.util.MD5Hash;



/**
 * @author Dongchk
 *
 */
public class OOSDBTest {

	private final String BUCKET_FILE_URL = "/ibdata/ibdata1/bucket.txt"; 
	private final  String OWNER_FILE_URL = "/ibdata/ibdata1/owner.txt"; 
	private final  String OBJECT_FILE_URL = "/ibdata/"; 
	
	private long bucketCount = 10;
	private long ownerCount = 1000;
	private long objectCount = 1000000;
	private long fileNum = 10;
	
	public OOSDBTest(){
		
	}
	

	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			new OOSDBTest().exec(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	
	public void exec(String[] args) throws IOException{
		File ownerFile = new File(OWNER_FILE_URL);
		if(ownerFile.exists()){
			ownerFile.delete();
		}
		FileWriter fw1 = new FileWriter(OWNER_FILE_URL);   
		File bucketFile = new File(BUCKET_FILE_URL);
		if(bucketFile.exists()){
			bucketFile.delete();
		}
		FileWriter fw2 = new FileWriter(BUCKET_FILE_URL);   
		FileWriter fw3 = null;
		long ownerId =0;
		long bucketId =0;
		long objectId =0;
		long count=0;
		long fileCount  = objectCount*bucketCount*ownerCount/fileNum;  //每个文件的object数量
		long fileId = 0;
		long ii=0;
		//写用户
		for(int i=0;i<ownerCount;i++){
			ownerId++;
			String[] owner = createOwner(i);
			try {
                write2File(fw1,owner);
            } finally {
                fw1.close();
            }
			//System.out.println("owner "+Integer.toString(ownerId));
			//写对象容器
			for(int j =0; j<bucketCount; j++){
				bucketId++;
				String bucketName = "bucket-"+bucketId;
				long id = getBucketID(bucketName);
				String bukid = Long.toString(id);
				String[] bucket = createBucket(bukid,bucketName,ownerId,bucketId);
				try {
                    write2File(fw2,bucket);
                } finally {
                    fw2.close();
                }
				//System.out.println("bucket "+bukid);
				//写对象
				for(int k=0;k<objectCount;k++){
					if((objectId%fileCount)==0||objectId==0){
						fileId++;
						String fileName = OBJECT_FILE_URL+"ibdata"+Long.toString(fileId)+"/object"+Long.toString(fileId)+".txt";
						File objectFile = new File(fileName);
						if(objectFile.exists()){
							objectFile.delete();
						}
						fw3 = new FileWriter(fileName); 
					}
					objectId++;
					String objectName = "object"+"-"+objectId+".txt";
					String objid = getObjectID(objectName);
					String[] object = createObject(objid,objectName,bukid,objectId);
					try {
                        write2File(fw3,object);
                    } finally {
                        if(fw3 != null)
                            fw3.close();
                    }
					System.out.println("object "+Long.toString(count++));
				}
			}
		}
	}
	
	
	public String[] createOwner(long num){
		String[] owner = new String[14];
		String temp =  Long.toString(num+1);
		long mis = getCurrentDateLong()+num;
		for(int i=0;i<14;i++){
			int a=0;
			owner[a++] = temp;
			owner[a++] = "crury"+temp;
			owner[a++] = "cuimeng"+ temp;
			owner[a++] = "01"+temp;
			owner[a++] = "crury"+temp+"@qq.com";
			owner[a++] = "ctyun"+temp;
			owner[a++] = "西山赢府"+temp;
			owner[a++] = "1891000"+temp;
			owner[a++] = "0102522"+temp;
			owner[a++] = "VEC"+temp;
			owner[a++] = "1"+temp;
			owner[a++] = Long.toString(mis);
			owner[a++] = "10"+temp;
			owner[a++] = Long.toString(mis);
		}
		return owner;
	}
	
	public String[] createBucket(String bucketId,String bucketName,long ownerId,long id){
		String[] bucket = new String[9];
		long mis = getCurrentDateLong();
		String temp  = Long.toString(id);
		for(int i=0;i<9;i++){
			int a=0;
			bucket[a++]=bucketId;
			bucket[a++]=bucketName;
			bucket[a++]=Long.toString(ownerId);
			bucket[a++]="china";
			bucket[a++]="private";
			bucket[a++]= Long.toString(mis);
			bucket[a++]="<xml>"+temp+"</xml>";
			bucket[a++]="index"+temp+".html";
			bucket[a++]="error"+temp+".html";
		}
		return bucket;
	}
	
	public String[] createObject(String objectId,String objectName,String bucketId,long id){
		String[] object = new String[13];
		long mis = getCurrentDateLong();
		String temp = Long.toString(id);
		for(int i=0;i<13;i++){
			int a=0;
			object[a++]=objectId;
			object[a++]=objectName;
			object[a++]=bucketId;
			object[a++]="200"+temp;
			object[a++]="ajlkfdjk"+temp;
			object[a++]=Long.toString(mis);
			object[a++]="meta"+temp;
			object[a++]="cacheControl`"+temp;
			object[a++]="contentEncoding"+temp;
			object[a++]="contentMD5"+temp;
			object[a++]="text/plain";
			object[a++]="contentDisposition"+temp;
			object[a++]=temp;
		}
		return object;
	}
	
	public long getBucketID(String name){
		return MD5Hash.digest(name).halfDigest();
	}
	
	public String getObjectID(String name){
		return MD5Hash.digest(name).toString();
	}

	public String getCurrentDateString(){
		return Long.toString(getCurrentDateLong());
	}
	
	public long getCurrentDateLong(){
		return System.currentTimeMillis();
	}
	
    /**
     * @throws IOException 
     */
    private void write2File(FileWriter  fw,String[] text) throws IOException{
    	String strLine = "";
       for(int i=0;i<text.length;i++){
    	   if(i!=text.length-1){
    		   strLine += text[i]+",";
    	   }else{
    		   strLine += text[i];
    	   }
    	   //fw.write(text[i]);
    	   //fw.write(",");
       }
       fw.write(strLine+"\n");
       //fw.write("\n");
    }
}
