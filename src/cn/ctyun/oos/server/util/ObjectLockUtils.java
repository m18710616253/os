package cn.ctyun.oos.server.util;

import static cn.ctyun.oos.common.ErrorMessage.ERROR_CODE_FILE_IMMUTABLE;
import static cn.ctyun.oos.common.ErrorMessage.ERROR_MESSAGE_FILE_IMMUTABLE;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.handler.codec.http.HttpMethod;

import com.google.common.collect.Lists;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.conf.OOSConfig;
import cn.ctyun.common.model.ObjectLockConfiguration;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSActions;
import cn.ctyun.oos.hbase.MetaClient;
import cn.ctyun.oos.metadata.BucketMeta;
import cn.ctyun.oos.metadata.ObjectMeta;
import cn.ctyun.oos.metadata.UploadMeta;
import cn.ctyun.oos.server.backoff.Backoff;
import common.tuple.Pair;

/**
 * 对象操作前进行 保留判别的工具类
 *
 * @author Mirt Zhang
 * @date 2020/6/22.
 */
public class ObjectLockUtils {

    private static final Log log = LogFactory.getLog(ObjectLockUtils.class);

    private final static MetaClient client = MetaClient.getGlobalClient();

    /**
     * 所有需要校验合规保留操作的列表
     * 此处 action分为三大类，删除delete 上传put post，校验原则一致，即不能删除已经存在的处于保留期内的对象，不能覆盖已经存在的对象
     */
    private final static List<OOSActions> LIMIT_ACTION = Lists.newArrayList(OOSActions.Object_Put, OOSActions.Object_Delete, OOSActions.Object_Post);

    /**
     * 用于操作对象前校验被操作对象是否在合规保留期内
     *
     * @param req
     * @param dbBucket
     * @param key
     * @throws BaseException
     * @throws IOException
     */
    public static void objectLockCheck(HttpServletRequest req, BucketMeta dbBucket, String key) throws BaseException, IOException {
        if (dbBucket.objectLockConfiguration == null) {
            // 没有对象锁定策略
            return;
        }
        if (!ObjectLockConfiguration.ObjectLockStatus.ENABLED.toString().equals(dbBucket.objectLockConfiguration.objectLockEnabled)) {
            // 没有启用对象保留策略
            return;
        }

        OOSActions action = getActionNeedCheck(req);
        if (!LIMIT_ACTION.contains(action)) {
            // 操作不在被限制合规保留的列表中，无需校验
            return;
        }

        Pair<Boolean, Object> objectExistsPair = objectExist(req, dbBucket, key);
        if (!objectExistsPair.first()) {
            // 操作的对象不存在，不需要校验合规保留内容
            return;
        }

        Pair<ChronoUnit, Integer> timePair = dbBucket.objectLockConfiguration.getRuleTimePair();
        if (objectCanOperate(objectExistsPair.second(), timePair)) {
            // 上次操作时间 + 保留天数 < 当前时间 已经过了对象保留时间，允许操作
            return;
        }
        // 没有通过合规保留校验 对象不允许被操作。
        throw new BaseException(403, ERROR_CODE_FILE_IMMUTABLE, ERROR_MESSAGE_FILE_IMMUTABLE);
    }

    /**
     * 用于校验已经获取过属性的对象
     * 是否在合规保留期内
     *
     * @param dbBucket 用户的bucket
     * @param object   该方法认为传入的objectMeta已经获得了相关信息，不再进行select
     * @return true :对象属于合规保留期内 不允许操作 false :对象不在保留期内 可以操作
     * @throws BaseException,IOException 对于对象操作有异常时抛出，应该对抛出的错误进行处理。
     */
    public static boolean objectLockCheck(BucketMeta dbBucket, ObjectMeta object) {
        if (dbBucket.objectLockConfiguration == null) {
            // 没有对象锁定策略
            return false;
        }
        if (!ObjectLockConfiguration.ObjectLockStatus.ENABLED.toString().equals(dbBucket.objectLockConfiguration.objectLockEnabled)) {
            // 没有启用对象保留策略
            return false;
        }
        ObjectMeta copiedObject = new ObjectMeta(object.name, object.bucketName, object.metaRegionName);
        copiedObject.lastModified = getObjectMetaLastModified(object, false);

        Pair<ChronoUnit, Integer> timePair = dbBucket.objectLockConfiguration.getRuleTimePair();
        if (objectCanOperate(copiedObject, timePair)) {
            // 上次操作时间 + 保留天数 < 当前时间 已经过了对象保留时间，允许操作
            return false;
        }
        log.info(String.format("ObjectLockUtils.multipleDeleteObjectLockCheck object can not delete!!! object name : %s, bucketName : %s, metaLocation : %s, object.lastModified : %s", object.name, dbBucket.getName(), dbBucket.metaLocation, object.lastModified));
        // 没有通过合规保留校验 对象不允许被操作。
        return true;
    }

    /**
     * 用于校验已经获取过属性的对象,并且已经获取了bucket上合规保留周期属性
     * 是否在合规保留期内
     *
     * @param timePair bucket上解析出来的保留周期属性
     * @param object 该方法认为传入的objectMeta已经获得了相关信息，不再进行select
     * @return false :对象属于合规保留期内 不允许操作 true :对象不在保留期内 可以操作
     */
    public static boolean objectLockCheck(Pair<ChronoUnit, Integer> timePair, ObjectMeta object) {
        ObjectMeta copiedObject = new ObjectMeta(object.name, object.bucketName, object.metaRegionName);
        copiedObject.lastModified = getObjectMetaLastModified(object, false);
        if (objectCanOperate(copiedObject, timePair)) {
            // 上次操作时间 + 保留天数 < 当前时间 已经过了对象保留时间，允许操作
            return true;
        }
        log.info(String.format("ObjectLockUtils.multipleDeleteObjectLockCheck object can not delete!!! object name : %s, object.lastModified : %s", object.name, object.lastModified));
        // 没有通过合规保留校验 对象不允许被操作。
        return false;
    }

    /**
     * 获取需要校验的 action
     *
     * @param req
     * @return 返回需要校验的action ，null代表该操作无需校验
     */
    private static OOSActions getActionNeedCheck(HttpServletRequest req) {
        OOSActions action = null;
        String method = req.getMethod();
        if (method.equalsIgnoreCase(HttpMethod.PUT.toString())) {
            action = OOSActions.Object_Put;
        } else if (method.equalsIgnoreCase(HttpMethod.DELETE.toString())) {
            action = OOSActions.Object_Delete;
        } else if (method.equalsIgnoreCase(HttpMethod.POST.toString())) {
            action = OOSActions.Object_Post;
        }
        return action;
    }

    /**
     * 获取操作的对象(可能是正常object 也可能是uploadPart)是否存在
     *
     * @param req
     * @param dbBucket
     * @param key
     * @return pair(是否存在对象 ， 具体对象)
     */
    private static Pair<Boolean, Object> objectExist(HttpServletRequest req, BucketMeta dbBucket, String key) throws BaseException, IOException {
        String method = req.getMethod();
        boolean isUploadPartRequest = HttpMethod.PUT.toString().equalsIgnoreCase(method) && req.getParameter("partNumber") != null && req.getParameter("uploadId") != null;
        if (isUploadPartRequest) {
            return uploadPartExist(req, dbBucket);
        } else {
            boolean isCompleteMultipartUpload = HttpMethod.POST.toString().equalsIgnoreCase(method) && req.getParameter("uploads") == null && req.getParameter("uploadId") != null;
            return normalObjectExist(dbBucket, key, isCompleteMultipartUpload);
        }
    }

    private static Pair<Boolean, Object> normalObjectExist(BucketMeta dbBucket, String key, boolean isCompleteMultipartUpload) throws BaseException, IOException {
        // 对对象操作进行合规保留校验
        ObjectMeta object = new ObjectMeta(key, dbBucket.getName(), dbBucket.metaLocation);
        if (client.isBusy(object, OOSConfig.getMaxConcurrencyPerRegionServer())) {
            Backoff.backoff();
        }
        boolean objectExists;
        try {
            objectExists = client.objectSelect(object);
        } catch (Exception e) {
            log.error("ObjectLockUtils.normalObjectExist select object error, key : " + key + " bucketName : " + dbBucket.getName() + " metaLocation : " + dbBucket.metaLocation, e);
            // 此处获取对象是否存在出现异常 出于安全考虑 进行抛错处理。
            throw new IOException(e);
        }
        long objectMetaLastModified = object.lastModified;
        object.lastModified = getObjectMetaLastModified(object, isCompleteMultipartUpload);
        log.info(String.format("ObjectLockUtils.normalObjectExist check object key : %s, bucketName : %s, metaLocation : %s, objectExists : %s, object.lastModified : %s, lastModified For ObjectLockCheck : %s", key, dbBucket.getName(), dbBucket.metaLocation, objectExists, objectMetaLastModified, object.lastModified));
        return new Pair<>(objectExists, object);
    }

    /**
     * 由于合并分片上传对象 需要特殊处理对象的lastModified时间，单独提供方法获取该时间。
     * 返回为当前object设置合规保留需要校验的时间
     *
     * @param object                    需要校验的对象,该方法内不要对object的属性进行任何修改！
     * @param isCompleteMultipartUpload 是否是合并操作
     * @return 用于合规保留校验的最后更新时间
     */
    private static long getObjectMetaLastModified(ObjectMeta object, boolean isCompleteMultipartUpload) {
        if (isCompleteMultipartUpload && StringUtils.isBlank(object.storageId)) {
            // 对未合并的分段上传对象进行合并 认为lastModified为0
            return 0L;
        }
        return object.lastModified;
    }

    private static Pair<Boolean, Object> uploadPartExist(HttpServletRequest req, BucketMeta dbBucket) throws BaseException, IOException {
        // 此处partNumber之所以用默认为0 是因为如果用户partNumber传输错误，让用户通过该合规保留校验，到后续上传分片的业务逻辑中会抛出partNumber异常的业务错误。
        UploadMeta upload = new UploadMeta(dbBucket.metaLocation, req.getParameter("uploadId"), NumberUtils.toInt(req.getParameter("partNumber"), 0));
        if (client.isBusy(upload, OOSConfig.getMaxConcurrencyPerRegionServer())) {
            Backoff.backoff();
        }
        boolean exist;
        try {
            exist = client.uploadSelect(upload);
        } catch (Exception e) {
            log.error("ObjectLockUtils.uploadPartExist select object error, uploadId : " + upload.initialUploadId + " partNumber :" + upload.partNum + " bucketName : " + dbBucket.getName() + " metaLocation : " + dbBucket.metaLocation, e);
            throw new IOException(e);
        }
        log.info(String.format("ObjectLockUtils.uploadPartExist check object uploadId : %s, bucketName : %s, metaLocation : %s, objectExists : %s, object.lastModified : %s", upload.initialUploadId, dbBucket.getName(), dbBucket.metaLocation, exist, upload.lastModified));
        return new Pair<>(exist, upload);
    }

    /**
     * 判断对象能否操作
     *
     * @param object   被校验的对象(ObjectMeta 或者 UploadMeta)
     * @param timePair 合规保留的时间
     * @return true 如果对象可以被操作。
     * false 对象不可被操作，或者被校验的对象不是ObjectMeta 或者 UploadMeta则恒返回false
     */
    public static boolean objectCanOperate(Object object, Pair<ChronoUnit, Integer> timePair) {
        long lastModified = System.currentTimeMillis();
        if (object instanceof ObjectMeta) {
            lastModified = (((ObjectMeta) object).lastModified);
        } else if (object instanceof UploadMeta) {
            lastModified = (((UploadMeta) object).lastModified);
        }
        LocalDateTime lastModifyTime = Instant.ofEpochMilli(lastModified).atZone(ZoneOffset.systemDefault()).toLocalDateTime();
        return lastModifyTime.plus(timePair.second(), timePair.first()).isBefore(LocalDateTime.now());
    }

    /**
     * 校验put ObjectLockConfiguration 解析出来的 conf是否合法
     * objectLockEnabled 为必填，必须设置是否启用
     * rule为非必填，不填写时不用再对子标签进行校验，
     * 如果填写了rule标签 则所有子标签均必须按要求填写，需要校验。
     * <p>
     * 标准标签样式如下：
     * <ObjectLockConfiguration>
     * <ObjectLockEnabled>Enabled</ObjectLockEnabled>
     * <Rule>
     * <DefaultRetention>
     * <Mode>COMPLIANCE</Mode>
     * <Days>days</Days>
     * <Years>years</Years>
     * </DefaultRetention>
     * </Rule>
     * </ObjectLockConfiguration>
     *
     * @param conf
     * @throws BaseException
     */
    public static void validCheckObjectLockConfiguration(ObjectLockConfiguration conf) throws BaseException {
        if (Objects.isNull(conf)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        // 不合法的状态
        if (!ObjectLockConfiguration.ObjectLockStatus.isValidName(conf.objectLockEnabled)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        // 允许不设置具体rule，如果没有设置则不再向下进行校验
        if (Objects.isNull(conf.rule)) {
            return;
        }
        // 已经设置了rule标签则必须对所有子标签进行校验
        ObjectLockConfiguration.DefaultRetention defaultRetention = conf.rule.defaultRetention;
        // 没有配置默认合规设置
        if (Objects.isNull(defaultRetention)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        // 不合法的 合规保留模式
        if (!ObjectLockConfiguration.ObjectLockRetentionMode.isValidName(defaultRetention.mode)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        // 没有设置保留时间
        if (Objects.isNull(defaultRetention.days) && Objects.isNull(defaultRetention.years)) {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        int days = Optional.ofNullable(defaultRetention.days).orElse(0);
        int years = Optional.ofNullable(defaultRetention.years).orElse(0);
        if (days > 0 && years > 0) {
            // 同时设置了 days years
            throw new BaseException(400, ErrorMessage.ERROR_CODE_MALFORMEDXML, ErrorMessage.ERROR_MESSAGE_MALFORMEDXML);
        }
        if (days <= 0 && years <= 0) {
            // 设置的 days years不是正整数
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_ARGUMENT_FOR_INVALID_VALUE_OF_DAY_OR_YEAR);
        }
        final int daysUpperLimit = 36500;
        final int yearsUpperLimit = 100;
        if (days > daysUpperLimit || years > yearsUpperLimit) {
            // 设置的days years超过上限
            throw new BaseException(400, ErrorMessage.ERROR_CODE_INVALID_ARGUMENT, ErrorMessage.ERROR_MESSAGE_INVALID_ARGUMENT_OVER_LIMIT);
        }
    }
}
