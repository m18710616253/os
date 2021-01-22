package cn.ctyun.oos.server.management;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CtyunGetUsageRequest;
import com.amazonaws.services.s3.model.CtyunGetUsageResult;
import com.amazonaws.services.s3.model.CtyunPoolUsage;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.common.Program;
import cn.ctyun.common.conf.GlobalHHZConfig;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.dsync.DLock;
import cn.ctyun.common.dsync.DSyncService;
import cn.ctyun.common.node.OosZKNode;
import cn.ctyun.common.utils.LogUtils;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.AkSkMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import cn.ctyun.oos.server.conf.QueryStorageConfig;
import common.time.TimeUtils;
import common.tuple.Pair;

public class QueryStorageTool implements Program {
    static {
        System.setProperty("log4j.log.app", "queryStorageTool");
    }
    private static Log log = LogFactory.getLog(QueryStorageTool.class);
    private MetaClient metaClient = MetaClient.getGlobalClient();
    private static String storageFileName = "storage.txt";
    private static ClientConfiguration cc = new ClientConfiguration();
    static {
        cc.setConnectionTimeout(30 * 1000);
        cc.setSocketTimeout(30 * 1000);
    }
    
    private long getStorage(AmazonS3Client client, Date date) {
        CtyunGetUsageRequest getUsageRequest = new CtyunGetUsageRequest();
        getUsageRequest.setBeginDate(date);
        getUsageRequest.setEndDate(date);
        CtyunGetUsageResult result = client.ctyunGetUsage(getUsageRequest);
        ArrayList<CtyunPoolUsage> usage = result.getDatailUsage();
        if (usage!= null && usage.size() > 0) {
            for (CtyunPoolUsage u:usage) {
                if (u.getPoolName() != null && u.getPoolName().equals(Consts.GLOBAL_DATA_REGION)) {
                    if (u.getCtyunPoolUsageData()!= null && u.getCtyunPoolUsageData().size() >0)
                        return u.getCtyunPoolUsageData().get(0).getCapacity();
                }
            }
            return 0L;
        } else {
            return 0L;
        }
    }
    
    private AmazonS3Client getManageClient(OwnerMeta owner) throws Exception {
        final AkSkMeta primaryAKSK = new AkSkMeta();
        primaryAKSK.ownerId = owner.getId();
        metaClient.akskSelectPrimaryKeyByOwnerId(primaryAKSK);
        AmazonS3Client client = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return primaryAKSK.accessKey;
            }
            
            public String getAWSSecretKey() {
                return primaryAKSK.getSecretKey();
            }
        }, cc);
        client.setEndpoint("http://" + QueryStorageConfig.manageAPIIP + ":" + QueryStorageConfig.manageAPIPort);
        return client;
    }
    
    private OwnerMeta getOwner(String ak) throws Exception {
        AkSkMeta asKey = new AkSkMeta(ak);
        if (!metaClient.akskSelect(asKey))
            throw new BaseException(403, "InvalidAccessKeyId");
        if (asKey.status == 0)
            throw new BaseException(403, "AccessDenied");
        OwnerMeta owner = new OwnerMeta(asKey.ownerId);
        if (!metaClient.ownerSelectById(owner))
            throw new BaseException(403, "InvalidAccessKeyId");
        return owner;
    }
    
    private AmazonS3Client getClient(final String ak, final String sk) throws Exception {
        AmazonS3Client client = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return ak;
            }
            
            public String getAWSSecretKey() {
                return sk;
            }
        }, cc);
        client.setEndpoint("http://" + OOSConfig.getDomainSuffix()[0]);
        return client;
    }
    
    private void queryStorage() throws Exception {
        for (Pair<String, String> user : QueryStorageConfig.users) {
            String ak = user.first();
            String bucketName = user.second();
            OwnerMeta owner = getOwner(ak);
            AmazonS3Client manageClient = getManageClient(owner);
            Date yesterDay = DateUtils.addDays(new Date(), -1);
            long capacity = getStorage(manageClient, yesterDay);
            AkSkMeta asKey = new AkSkMeta(ak);
            metaClient.akskSelect(asKey);
            AmazonS3Client client = getClient(ak, asKey.getSecretKey());
            String key = ak + "/" + storageFileName;
            S3Object object = null;
            long objectLength = 0;
            StringBuilder content = new StringBuilder();
            Vector<InputStream> vector = new Vector<InputStream>();
            InputStream input = null;
            try {
                try {
                    object = client.getObject(bucketName, key);
                    objectLength = object.getObjectMetadata().getContentLength();
                    input = object.getObjectContent();
                    vector.addElement(input);
                } catch (AmazonServiceException e) {
                    log.error(e.getMessage(), e);
                }
                if (object != null)
                    content.append("\n");
                content.append(TimeUtils.toYYYYMMDD(yesterDay)).append(" ")
                        .append(String.valueOf(capacity));
                try (InputStream in = IOUtils.toInputStream(content.toString(), Consts.STR_UTF8)) {
                    vector.addElement(in);
                    try (SequenceInputStream sis = new SequenceInputStream(vector.elements())) {
                        ObjectMetadata meta = new ObjectMetadata();
                        meta.setContentLength(content.length() + objectLength);
                        client.putObject(bucketName, key, sis, meta);
                        log.info("put object success, date: " + yesterDay + ", capacity: "
                                + capacity);
                    }
                }
            } finally {
                //此处会出现多次close流的情况，该情况可不作处理
                if (input != null)
                    input.close();
            }
        }
    }
    
    @Override
    public String usage() {
        return "query storage";
    }
    
    @Override
    public void exec(String[] args) throws Exception {
        DSyncService dsync = new DSyncService(GlobalHHZConfig.getQuorumServers(),
                GlobalHHZConfig.getSessionTimeout());
        try {
            DLock lock = dsync.createLock(OosZKNode.queryStorageLock, null);
            lock.lock();
            LogUtils.startSuccess();
            try {
                Calendar calendar = Calendar.getInstance();
                while (true) {
                    Date now = new Date();
                    calendar.setTime(now);
                    if (calendar.get(Calendar.HOUR_OF_DAY) == Integer
                            .parseInt(QueryStorageConfig.runtime.split(":")[0])
                            && calendar.get(Calendar.MINUTE) < Integer
                                    .parseInt(QueryStorageConfig.runtime.split(":")[1])
                            && calendar.get(Calendar.MINUTE) > (Integer
                                    .parseInt(QueryStorageConfig.runtime.split(":")[1]) - 5)) {
                        try {
                            queryStorage();
                            Thread.sleep(Consts.SLEEP_TIME * 1000);
                        } catch (Throwable e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    try {
                        Thread.sleep(Consts.ONE_MINITE);
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
            } finally {
                lock.unlock();
            }
        } finally {
            dsync.close();
        }
    }
    
    public static void main(String[] args) {
        try {
            new QueryStorageTool().exec(args);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
