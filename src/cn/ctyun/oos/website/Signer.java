package cn.ctyun.oos.website;

import cn.ctyun.common.BaseException;
import cn.ctyun.oos.common.Utils;
import cn.ctyun.oos.server.signer.V4Signer;
import cn.ctyun.oos.tempo.AdaptorClientV4;

import java.net.URL;
import java.util.Map;

public class Signer {

    public static String signV4Iam(com.amazonaws.Request<?> oosRequest, String accessKey, String secretKey, URL url,
            String method, String dateTimeStamp, String serviceName) throws BaseException {
        Map<String, String> headers = oosRequest.getHeaders();
        for (String key : headers.keySet()) {
            if (key.contains("x-ctyun"))
                continue;
        }
        headers.remove("Connection");
        headers.remove("Content-Length");
        String regionName = Utils
                .getRegionNameFromReqHost(oosRequest.getHeaders().get("Host"), serviceName);
        String authorization = AdaptorClientV4
                .computeSignature(headers, oosRequest.getParameters(), V4Signer.UNSIGNED_PAYLOAD, accessKey, secretKey,
                        url, method, serviceName, regionName, dateTimeStamp);
        return authorization;
    }
}
