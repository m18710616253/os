package cn.ctyun.oos.server.stress;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import common.util.GetOpt;

public class Benchmark {
    public static void main(String[] args) {
        GetOpt opts = new GetOpt("s:[bn]:[on]:c:f:", args);
        String url = opts.getOpt("s");
        String bucket = opts.getOpt("bn");
        String object = opts.getOpt("on");
        int count = opts.getInt("c", 0);
        String path = opts.getOpt("f");
        AmazonS3 s3 = new AmazonS3Client();
        s3.setEndpoint(url);
        AccessControlList acl = s3.getBucketAcl(bucket);
    }
}
