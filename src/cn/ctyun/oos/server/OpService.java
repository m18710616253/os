package cn.ctyun.oos.server;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.internal.XmlWriter;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.OwnerMeta;
import common.tuple.Pair;

/**
 * @author: Cui Meng
 */
public class OpService {
    private static final Log log = LogFactory.getLog(OpService.class);

    public static String listAllMyBucketsResult(OwnerMeta owner, List<BucketMeta> list) {
        XmlWriter xml = new XmlWriter();
        xml.start("ListAllMyBucketsResult", "xmlns", "http://doc.s3.amazonaws.com/2006-03-01");
        xml.start("Owner");
        xml.start("ID").value(owner.getName()).end();
        if (owner.displayName != null && owner.displayName.trim().length() != 0)
            xml.start("DisplayName").value(owner.displayName).end();
        else
            xml.start("DisplayName").value("").end();
        xml.end();
        xml.start("Buckets");
        for (BucketMeta bucket : list) {
            xml.start("Bucket");
            xml.start("Name").value(bucket.getName()).end();
            xml.start("CreationDate")
                    .value(ServiceUtils.formatIso8601Date(new Date(bucket.createDate))).end();
            xml.end();
        }
        xml.end();
        xml.end();
        if (log.isDebugEnabled())
            log.debug(xml.toString());
        String str = Consts.XML_HEADER + xml.toString();
        str = str.replaceAll("&quote;", "&quot;");
        return str;
    }

    public static String getRegions(Pair<Set<String>, Set<String>> regions)
            throws IOException, BaseException {
        XmlWriter xml = new XmlWriter();
        xml.start("BucketRegions");
        if (regions.first().size() > 0) {
            xml.start("MetadataRegions");
            for (String r : regions.first())
                xml.start("Region").value(r).end();
            xml.end();
        }
        if (regions.second().size() > 0) {
            xml.start("DataRegions");
            for (String r : regions.second())
                xml.start("Region").value(r).end();
            xml.end();
        }
        xml.end();
        String str = Consts.XML_HEADER + xml.toString();
        return str;
    }
}
