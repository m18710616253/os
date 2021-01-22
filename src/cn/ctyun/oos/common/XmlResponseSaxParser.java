package cn.ctyun.oos.common;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.RedirectRule;
import com.amazonaws.services.s3.model.RoutingRule;
import com.amazonaws.services.s3.model.RoutingRuleCondition;
import com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.model.BucketLifecycleConfiguration;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Rule;
import cn.ctyun.common.model.BucketLifecycleConfiguration.Transition;
import cn.ctyun.common.model.ObjectLockConfiguration;
import cn.ctyun.oos.metadata.AkSkMeta;

public class XmlResponseSaxParser {
    private static final Log log = LogFactory.getLog(XmlResponsesSaxParser.class);
    private XMLReader xr = null;
    private boolean sanitizeXmlDocument = true;
    
    public XmlResponseSaxParser() throws AmazonClientException {
        // Ensure we can load the XML Reader.
        try {
            xr = XMLReaderFactory.createXMLReader();
        } catch (SAXException e) {
            // oops, lets try doing this (needed in 1.4)
            System.setProperty("org.xml.sax.driver", "org.apache.crimson.parser.XMLReaderImpl");
            try {
                // Try once more...
                xr = XMLReaderFactory.createXMLReader();
            } catch (SAXException e2) {
                throw new AmazonClientException(
                        "Couldn't initialize a sax driver for the XMLReader");
            }
        }
    }
    
    public CompleteUploadHandler parseCompleteUpload(InputStream inputStream)
            throws AmazonClientException {
        CompleteUploadHandler handler = new CompleteUploadHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }
    
    public ListAccessKeyHandler parseListAccessKey(InputStream inputStream)
            throws AmazonClientException {
        ListAccessKeyHandler handler = new ListAccessKeyHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }
    
    public CreateAccessKeyHandler parseCreateAccessKey(InputStream inputStream)
            throws AmazonClientException {
        CreateAccessKeyHandler handler = new CreateAccessKeyHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }
    
    public PutBucketLoggingHandler parsePutBucketLogging(InputStream inputStream)
            throws AmazonClientException {
        PutBucketLoggingHandler handler = new PutBucketLoggingHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }
    
    public DeleteMultipleObjectsHandler parseDeleteMultipleObjects(InputStream inputStream)
            throws AmazonClientException {
        DeleteMultipleObjectsHandler handler = new DeleteMultipleObjectsHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }

    public DeleteMultipleVersionObjectsByPrefixHandler parseDeleteMultipleVersionByPrefixObjects(InputStream inputStream)
            throws AmazonClientException {
        DeleteMultipleVersionObjectsByPrefixHandler handler = new DeleteMultipleVersionObjectsByPrefixHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }

    public DeleteMultipleVersionObjectsHandler parseDeleteMultipleVersionObjects(InputStream inputStream)
            throws AmazonClientException {
        DeleteMultipleVersionObjectsHandler handler = new DeleteMultipleVersionObjectsHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }

    public BucketLifecycleConfigurationHandler parseBucketLifecycleConfigurationResponse(
            InputStream inputStream) {
        BucketLifecycleConfigurationHandler handler = new BucketLifecycleConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public ObjectLockConfigurationHandler parseObjectLockConfigurationResponse(InputStream inputStream) {
        ObjectLockConfigurationHandler handler = new ObjectLockConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    protected void parseXmlInputStream(DefaultHandler handler, InputStream inputStream)
            throws AmazonClientException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Parsing XML response document with handler: " + handler.getClass());
            }
            BufferedReader breader = new BufferedReader(new InputStreamReader(inputStream,
                    Constants.DEFAULT_ENCODING));
            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);
            xr.parse(new InputSource(breader));
        } catch (Throwable t) {
            throw new AmazonClientException("Failed to parse XML document with handler "
                    + handler.getClass(), t);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Unable to close response InputStream up after XML parse failure", e);
                }
            }
        }
    }
    
    protected InputStream sanitizeXmlDocument(DefaultHandler handler, InputStream inputStream)
            throws AmazonClientException {
        if (!sanitizeXmlDocument) {
            // No sanitizing will be performed, return the original input stream
            // unchanged.
            return inputStream;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Sanitizing XML document destined for handler " + handler.getClass());
            }
            InputStream sanitizedInputStream = null;
            try {
                /*
                 * Read object listing XML document from input stream provided
                 * into a string buffer, so we can replace troublesome
                 * characters before sending the document to the XML parser.
                 */
                StringBuilder listingDocBuffer = new StringBuilder();
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream,
                        Constants.DEFAULT_ENCODING));
                char[] buf = new char[8192];
                int read = -1;
                while ((read = br.read(buf)) != -1) {
                    listingDocBuffer.append(buf, 0, read);
                }
                /*
                 * Replace any carriage return (\r) characters with explicit XML
                 * character entities, to prevent the SAX parser from
                 * misinterpreting 0x0D characters as 0x0A and being unable to
                 * parse the XML.
                 */
                String listingDoc = listingDocBuffer.toString().replaceAll("\r", "&#013;");
                sanitizedInputStream = new ByteArrayInputStream(
                        listingDoc.getBytes(Constants.DEFAULT_ENCODING));
            } catch (Throwable t) {
                if (sanitizedInputStream != null)
                    try {
                        sanitizedInputStream.close();
                    } catch (Throwable e) {
                        if (log.isErrorEnabled()) {
                            log.error(
                                    "Unable to close response InputStream after failure sanitizing XML document",
                                    e);
                        }
                    }
                throw new AmazonClientException(
                        "Failed to sanitize XML document destined for handler "
                                + handler.getClass(), t);
            }
            return sanitizedInputStream;
        }
    }
    
    public class CompleteUploadHandler extends DefaultHandler {
        private List<PartETag> partETags = null;
        private StringBuilder currText = null;
        private PartETag currentPart = null;
        
        public CompleteUploadHandler() {
            super();
            partETags = new ArrayList<PartETag>();
            this.currText = new StringBuilder();
        }
        
        /**
         * @return the buckets listed in the document.
         */
        public List<PartETag> getPartETag() {
            return partETags;
        }
        
        /**
         * @return the owner of the buckets.
         */
        public void startDocument() {
        }
        
        public void endDocument() {
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
            if (name.equals("Part")) {
                currentPart = new PartETag(0, "");
            }
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            // Listing details.
            if (name.equals("PartNumber")) {
                currentPart.setPartNumber(Integer.parseInt(elementText));
            } else if (name.equals("ETag")) {
                currentPart.setETag(elementText);
            } else if (name.equals("Part")) {
                partETags.add(currentPart);
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }
    
    public class PutBucketLoggingHandler extends DefaultHandler {
        private BucketLoggingConfiguration logging = null;
        private StringBuilder currText = null;
        private Set<String> requiredFields = Stream.of("BucketLoggingStatus").collect(Collectors.toSet());
        private Set<String> xmlContainedFields = new HashSet<String>();

        public PutBucketLoggingHandler() {
            super();
            logging = new BucketLoggingConfiguration();
            this.currText = new StringBuilder();
        }
        
        /**
         * @return the buckets listed in the document.
         */
        public BucketLoggingConfiguration getLogging() throws BaseException{
            filterElement(requiredFields, xmlContainedFields);
            return logging;
        }
        
        /**
         * @return the owner of the buckets.
         */
        public void startDocument() {
        }
        
        public void endDocument(){
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
            xmlContainedFields.add(name);
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            if (name.equals("TargetBucket")) {
                logging.setDestinationBucketName(elementText);
            } else if (name.equals("TargetPrefix")) {
                logging.setLogFilePrefix(elementText);
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }
    
    public class DeleteMultipleObjectsHandler extends DefaultHandler {
        private List<KeyVersion> objects = null;
        private StringBuilder currText = null;
        private KeyVersion currentObject = null;
        private DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest("");
        
        public DeleteMultipleObjectsHandler() {
            super();
            objects = new ArrayList<KeyVersion>();
            this.currText = new StringBuilder();
        }
        
        public DeleteObjectsRequest getDeleteObjectsRequest() {
            return deleteObjectsRequest;
        }
        
        public void startDocument() {
        }
        
        public void endDocument() {
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            // Listing details.           
            if (name.equals("Key")) {
                if ((elementText==null) || (elementText.length()==0)) return;
                currentObject = new KeyVersion(elementText);
            }
            else if (name.equals("Object")) {
                objects.add(currentObject);
                currentObject = null;
            } else if (name.equals("Quiet")) {
                deleteObjectsRequest.setQuiet(Boolean.parseBoolean(elementText));
            } else if (name.equals("Delete")) {
                deleteObjectsRequest.setKeys(objects);
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }

    public class DeleteMultipleVersionObjectsHandler extends DefaultHandler {
        private List<DeleteMultipleVersionObj.KeyVersion> objects;
        private StringBuilder currText;
        private DeleteMultipleVersionObj.KeyVersion currentObject = null;
        private DeleteMultipleVersionObj multipleVersionObjects = new DeleteMultipleVersionObj();

        public DeleteMultipleVersionObjectsHandler() {
            super();
            objects = new ArrayList<>();
            this.currText = new StringBuilder();
        }

        public DeleteMultipleVersionObj getDeleteMultipleVersionObj() {
            return multipleVersionObjects;
        }

        public void startDocument() {
        }

        public void endDocument() {
        }

        public void startElement(String uri, String name, String qName, Attributes attrs) {
        }

        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            // Listing details.
            if (name.equals(DeleteMultipleVersionObjectsConstant.KEY)) {
                if (StringUtils.isBlank(elementText)) {
                    return;
                }
                currentObject = new DeleteMultipleVersionObj.KeyVersion();
                currentObject.setKey(elementText);
            }else if(name.equals(DeleteMultipleVersionObjectsConstant.VERSIONID)) {
                if (currentObject == null) {
                    currentObject = new DeleteMultipleVersionObj.KeyVersion();
                }
                currentObject.setVersion(Long.parseLong(elementText));
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.OBJECT)) {
                objects.add(currentObject);
                currentObject = null;
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.QUIET)) {
                multipleVersionObjects.setQuiet(Boolean.parseBoolean(elementText));
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.ERROR_CONTINUE)) {
                multipleVersionObjects.setErrorContinue(Boolean.parseBoolean(elementText));
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.DELETE)) {
                multipleVersionObjects.setObjects(objects);
            }
            this.currText = new StringBuilder();
        }

        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }

    public class DeleteMultipleVersionObjectsByPrefixHandler extends DefaultHandler {
        private StringBuilder currText = null;
        private DeleteMultipleVersionObjByPrefix prefix =
                new DeleteMultipleVersionObjByPrefix();

        public DeleteMultipleVersionObjectsByPrefixHandler() {
            super();
            this.currText = new StringBuilder();
        }

        public DeleteMultipleVersionObjByPrefix getDeleteMultipleVersionObjByPrefix() {
            return prefix;
        }

        public void startDocument() {
        }

        public void endDocument() {
        }

        public void startElement(String uri, String name, String qName, Attributes attrs) {
        }

        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            if (name.equals(DeleteMultipleVersionObjectsConstant.PREFIX)) {
                prefix.setPrefix(elementText);
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.MARKER)) {
                prefix.setMarker(elementText);
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.MAX_KEYS)) {
                prefix.setMaxKey(Integer.parseInt(elementText));
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.VERSIONID)) {
                prefix.setVersionId(Long.parseLong(elementText));
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.ERROR_CONTINUE)) {
                prefix.setErrorContinue(Boolean.parseBoolean(elementText));
            } else if (name.equals(DeleteMultipleVersionObjectsConstant.QUIET)) {
                prefix.setQuiet(Boolean.parseBoolean(elementText));
            }
            this.currText = new StringBuilder();
        }

        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }

    public class ListAccessKeyHandler extends DefaultHandler {
        private List<AkSkMeta> asKey = null;
        private StringBuilder currText = null;
        private AkSkMeta currentKey = null;
        private boolean isTruncated = false;
        private String marker = null;
        
        public ListAccessKeyHandler() {
            super();
            asKey = new ArrayList<AkSkMeta>();
            this.currText = new StringBuilder();
        }
        
        /**
         * @return the buckets listed in the document.
         */
        public List<AkSkMeta> getKeys() {
            return asKey;
        }
        
        public boolean getIsTruncated() {
            return isTruncated;
        }
        
        public String getMarker() {
            return marker;
        }
        
        /**
         * @return the owner of the buckets.
         */
        public void startDocument() {
        }
        
        public void endDocument() {
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
            if (name.equals("member")) {
                currentKey = new AkSkMeta();
            }
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            if (name.equals("AccessKeyId")) {
                currentKey.accessKey = elementText;
            } else if (name.equals("SecretAccessKey")) {
                currentKey.setSecretKey(elementText);
            } else if (name.equals("Status")) {
                if (elementText.equals("Active"))
                    currentKey.status = 1;
                else
                    currentKey.status = 0;
            } else if (name.equals("IsPrimary")) {
                if (elementText.equals("true"))
                    currentKey.isPrimary = 1;
                else
                    currentKey.isPrimary = 0;
            } else if (name.equals("member")) {
                asKey.add(currentKey);
            } else if (name.equals("IsTruncated")) {
                try {
                    isTruncated = Boolean.parseBoolean(elementText);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            } else if (name.equals("Marker")) {
                marker = elementText;
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }
    
    public class CreateAccessKeyHandler extends DefaultHandler {
        private StringBuilder currText = null;
        private AkSkMeta currentKey = null;
        
        public CreateAccessKeyHandler() {
            super();
            this.currText = new StringBuilder();
        }
        
        /**
         * @return the buckets listed in the document.
         */
        public AkSkMeta getKey() {
            return currentKey;
        }
        
        /**
         * @return the owner of the buckets.
         */
        public void startDocument() {
        }
        
        public void endDocument() {
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
            if (name.equals("AccessKey")) {
                currentKey = new AkSkMeta(1);
            }
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            if (name.equals("AccessKeyId")) {
                currentKey.accessKey = elementText;
            } else if (name.equals("SecretAccessKey")) {
                currentKey.setSecretKey(elementText);
            } else if (name.equals("Status")) {
                if (elementText.equals("Active"))
                    currentKey.status = 1;
                else
                    currentKey.status = 0;
            } else if (name.equals("IsPrimary")) {
                if (elementText.equals("true"))
                    currentKey.isPrimary = 1;
                else
                    currentKey.isPrimary = 0;
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }

    /**
     * 集合A中所有元素是否都存在B中。 不存在则抛 400 exception
     * 换句话说，B是否包含A的所有元素。
     * */
    private static void filterElement(Collection<?> a ,Collection<?> b) throws BaseException{
        Iterator<?> it = a.iterator();
        while(it.hasNext()) {
            Object obj = it.next();
            if(!b.contains(obj)) {
                throw new BaseException(400,"BadRequest","'"+obj.toString() + "' element is requested");
            }
        }
    }

    public BucketWebsiteConfigurationHandler parseWebsiteConfigurationResponse(InputStream inputStream)
            throws AmazonClientException
    {
        BucketWebsiteConfigurationHandler handler = new BucketWebsiteConfigurationHandler();
        parseXmlInputStream(handler, inputStream);
        return handler;
    }

    public static class BucketWebsiteConfigurationHandler extends DefaultHandler {
        private BucketWebsiteConfiguration configuration = new BucketWebsiteConfiguration(null);
        private RoutingRuleCondition condition = null;
        private RedirectRule redirect  = null;
        private List<RoutingRule> rules = null;
        private RedirectRule  redirectAllRequestsTo = null;
        RoutingRule rule = null;
        private StringBuilder text;

        boolean inIndexDocumentElement = false;
        boolean inErrorDocumentElement = false;
        boolean inRoutingRules = false;
        boolean inRoutingRule = false;
        boolean inCondition = false;
        boolean inRedirect = false;
        boolean inRedirectAllRequestsTo = false;

        public BucketWebsiteConfiguration getConfiguration() { return configuration; }

        @Override
        public void startDocument() {
            text = new StringBuilder();
        }

        @Override
        public void startElement(String uri, String name, String qName, Attributes attrs) {
            text.setLength(0);
            if ("WebsiteConfiguration".equals(name)) {
            } else if ("IndexDocument".equals(name)) {
                inIndexDocumentElement = true;
            } else if ("Suffix".equals(name) && inIndexDocumentElement) {
            } else if ("ErrorDocument".equals(name)) {
                inErrorDocumentElement = true;
            } else if ("Key".equals(name) && inErrorDocumentElement) {
            } else if ("RedirectAllRequestsTo".equals(name)) {
                redirectAllRequestsTo = new RedirectRule();
                inRedirectAllRequestsTo = true;
            } else if ("RoutingRules".equals(name)) {
                rules = new LinkedList<>();
                inRoutingRules = true;
            } else if ("RoutingRule".equals(name) && inRoutingRules) {
                rule = new RoutingRule();
                inRoutingRule = true;
            } else if ("Condition".equals(name) && inRoutingRule) {
                condition = new RoutingRuleCondition();
                inCondition = true;
            } else if ("KeyPrefixEquals".equals(name) && inCondition) {
            } else if ("HttpErrorCodeReturnedEquals".equals(name) && inCondition) {
            } else if ("Redirect".equals(name) && inRoutingRule) {
                redirect = new RedirectRule();
                inRedirect = true;
            } else if ("Protocol".equals(name) && (inRedirect || inRedirectAllRequestsTo)) {
            } else if ("HostName".equals(name) && (inRedirect || inRedirectAllRequestsTo)) {
            } else if ("ReplaceKeyPrefixWith".equals(name) && (inRedirect || inRedirectAllRequestsTo)) {
            } else if ("ReplaceKeyWith".equals(name) && (inRedirect || inRedirectAllRequestsTo)) {
            } else if ("HttpRedirectCode".equals(name) && (inRedirect || inRedirectAllRequestsTo)) {
            } else {
                log.warn("Ignoring unexpected tag <"+name+">");
            }
        }

        @Override
        public void endElement(String uri, String name, String qName) throws SAXException {
            if ("WebsiteConfiguration".equals(name)) {
            } else if ("IndexDocument".equals(name)) {
                inIndexDocumentElement = false;
            } else if ("Suffix".equals(name) && inIndexDocumentElement) {
                configuration.setIndexDocumentSuffix(text.toString());
            } else if ("ErrorDocument".equals(name)) {
                inErrorDocumentElement = false;
            } else if ("Key".equals(name) && inErrorDocumentElement) {
                configuration.setErrorDocument(text.toString());
            } else if ("KeyPrefixEquals".equals(name) && inCondition) {
                condition.setKeyPrefixEquals(text.toString());
            } else if ("HttpErrorCodeReturnedEquals".equals(name) && inCondition) {
                condition.setHttpErrorCodeReturnedEquals(text.toString());
            } else if ("Condition".equals(name) && inRoutingRule) {
                rule.setCondition(condition);
                inCondition = false;
                condition = null;
            } else if ("Protocol".equals(name) && inRedirect) {
                redirect.setProtocol(text.toString());
            } else if ("Protocol".equals(name) && inRedirectAllRequestsTo) {
                redirectAllRequestsTo.setProtocol(text.toString());
            } else if ("HostName".equals(name) && inRedirect) {
                redirect.setHostName(text.toString());
            } else if ("HostName".equals(name) && inRedirectAllRequestsTo) {
                redirectAllRequestsTo.setHostName(text.toString());
            } else if ("ReplaceKeyPrefixWith".equals(name) && inRedirect) {
                redirect.setReplaceKeyPrefixWith(text.toString());
            } else if ("ReplaceKeyPrefixWith".equals(name) && inRedirectAllRequestsTo) {
                redirectAllRequestsTo.setReplaceKeyPrefixWith(text.toString());
            } else if ("ReplaceKeyWith".equals(name) && inRedirect) {
                redirect.setReplaceKeyWith(text.toString());
            } else if ("ReplaceKeyWith".equals(name) && inRedirectAllRequestsTo) {
                redirectAllRequestsTo.setReplaceKeyWith(text.toString());
            } else if ("HttpRedirectCode".equals(name) && inRedirect) {
                redirect.setHttpRedirectCode(text.toString());
            } else if ("HttpRedirectCode".equals(name) && inRedirectAllRequestsTo) {
                redirectAllRequestsTo.setHttpRedirectCode(text.toString());
            } else if ("Redirect".equals(name) && inRoutingRule) {
                rule.setRedirect(redirect);
                inRedirect = false;
                redirect = null;
            } else if ("RoutingRule".equals(name) && inRoutingRules) {
                rules.add(rule);
                rule = null;
                inRoutingRule = false;
            } else if ("RoutingRules".equals(name)) {
                configuration.setRoutingRules(rules);
                rules = null;
                inRoutingRules = false;
            } else if ("RedirectAllRequestsTo".equals(name)) {
                configuration.setRedirectAllRequestsTo(redirectAllRequestsTo);
                inRedirectAllRequestsTo = false;
                redirectAllRequestsTo = null;
            }
            text.setLength(0);
        }

        @Override
        public void characters(char ch[], int start, int length) {
            this.text.append(ch, start, length);
        }
    }

    /*
    HTTP/1.1 200 OK
    x-amz-id-2: Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==
    x-amz-request-id: 656c76696e6727732072657175657374
    Date: Tue, 20 Sep 2012 20:34:56 GMT
    Content-Length: xxx
    Connection: keep-alive
    Server: AmazonS3

  <LifecycleConfiguration>
      <Rule>
          <ID>logs-rule</ID>
          <Prefix>logs/</Prefix>
          <Status>Enabled</Status>
          <Transition>
              <Days>30</Days>
              <StorageClass>GLACIER</StorageClass>
          </Transition>
          <Expiration>
              <Days>365</Days>
          </Expiration>
     </Rule>
     <Rule>
         <ID>image-rule</ID>
         <Prefix>image/</Prefix>
         <Status>Enabled</Status>
         <Transition>
             <Date>2012-12-31T00:00:00.000Z</Date>
             <StorageClass>GLACIER</StorageClass>
         </Transition>
         <Expiration>
             <Date>2020-12-31T00:00:00.000Z</Date>
         </Expiration>
     </Rule>
  </LifecycleConfiguration>
    */
    public class BucketLifecycleConfigurationHandler extends DefaultHandler {
        private StringBuilder text;
        private Rule rule;
        private List<Rule> rules = new LinkedList<Rule>();
        private Transition transition;
        boolean inTransition = false;

        public BucketLifecycleConfiguration getConfiguration() {
            return new BucketLifecycleConfiguration(rules);
        }

        @Override
        public void startDocument() {
            text = new StringBuilder();
        }

        @Override
        public void startElement(String uri, String name, String qName, Attributes attrs) throws SAXException {
            if (name.equals("LifecycleConfiguration")) {
            } else if (name.equals("Rule")) {
                rule = new Rule();
            } else if (name.equals("ID")) {
                if (rule != null && StringUtils.isNotEmpty(rule.getId())) {
                    throw new SAXException();
                }
            } else if (name.equals("Prefix")) {
                if (rule != null && StringUtils.isNotEmpty(rule.getPrefix())) {
                    throw new SAXException();
                }
            } else if (name.equals("Status")) {
                if (rule != null && StringUtils.isNotEmpty(rule.getStatus())) {
                    throw new SAXException();
                }
            } else if (name.equals("Transition")) {
                if (rule != null && rule.getTransition()!=null) {
                    throw new SAXException();
                }
                transition = new Transition();
                inTransition = true;
            } else if (name.equals("StorageClass")) {
                if (!inTransition || StringUtils.isNotEmpty(transition.getStorageClass())) {
                    throw new SAXException();
                }
            } else if (name.equals("Date")) {
                if (inTransition) {
                    if (transition != null && transition.getDate() != null) {
                        throw new SAXException();
                    }
                }else{
                    if (rule != null && rule.getExpirationDate() != null) {
                        throw new SAXException();
                    }
                }
            } else if (name.equals("Expiration")) {
                if (rule != null && rule.getExpirationDate() != null) {
                    throw new SAXException();
                }
            } else if (name.equals("Days")) {
                if (inTransition) {
                    if (transition != null && transition.getDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                        throw new SAXException();
                    }
                } else {
                    if (rule != null && rule.getExpirationInDays() != BucketLifecycleConfiguration.INITIAL_DAYS) {
                        throw new SAXException();
                    }
                }
            } else {
                log.warn("Unexpected tag: " + name);
            }
            text.setLength(0);
        }

        @Override
        public void endElement(String uri, String name, String qName) throws SAXException {
            if ( name.equals("LifecycleConfiguration") ) {
            } else if ( name.equals("Rule") ) {
                rules.add(rule);
                rule = null;
            } else if ( name.equals("ID") ) {
                rule.setId(text.toString());
            } else if ( name.equals("Prefix") ) {
                rule.setPrefix(text.toString());
            } else if ( name.equals("Status") ) {
                rule.setStatus(text.toString());
            } else if (name.equals("Transition")) {
                rule.setTransition(transition);
                transition = null;
                inTransition = false;
            } else if (name.equals("StorageClass")) {
              transition.setStorageClass(text.toString());
            } else if (name.equals("Date")) {
                if (!inTransition) {
                    try {
                        rule.setExpirationDate(ServiceUtils.parseIso8601Date(text.toString()));
                    } catch (ParseException e) {
                       rule.setExpirationDate(BucketLifecycleConfiguration.INVALID_DATE);
                    }
                } else {
                    try {
                        transition.setDate(ServiceUtils.parseIso8601Date(text.toString()));
                    } catch (ParseException e) {
                        transition.setDate(BucketLifecycleConfiguration.INVALID_DATE);
                    }
                }
            } else if (name.equals("Expiration")) {
            } else if (name.equals("Days")) {
                if (!inTransition) {
                    try {
                        int days = Integer.parseInt(text.toString());
                        if (days <= 0) {
                            days = BucketLifecycleConfiguration.INVALID_DAYS;
                        }
                        rule.setExpirationInDays(days);
                    } catch (NumberFormatException e) {
                        rule.setExpirationInDays(BucketLifecycleConfiguration.INVALID_DAYS);
                    }
                } else {
                    try {
                        int days = Integer.parseInt(text.toString());
                        if (days <= 0) {
                            days = BucketLifecycleConfiguration.INVALID_DAYS;
                        }
                        transition.setDays(days);
                    } catch (NumberFormatException e) {
                        transition.setDays(BucketLifecycleConfiguration.INVALID_DAYS);
                    }
                }
            } else {
                log.warn("Unexpected tag: " + name);
            }
        }

        @Override
        public void characters(char ch[], int start, int length) {
            this.text.append(ch, start, length);
        }
    }

    public static class ObjectLockConfigurationHandler extends DefaultHandler {

        private final StringBuilder text = new StringBuilder();
        private final LinkedList<String> context = new LinkedList<>();

        private ObjectLockConfiguration objectLockConfiguration = new ObjectLockConfiguration();
        private ObjectLockConfiguration.ObjectLockRule rule;
        private ObjectLockConfiguration.DefaultRetention defaultRetention;

        public ObjectLockConfiguration getObjectLockConfiguration() {
            return this.objectLockConfiguration;
        }

        @Override
        public final void startElement(
                String uri,
                String name,
                String qName,
                Attributes attrs) {

            text.setLength(0);
            doStartElement(uri, name, qName, attrs);
            context.add(name);
        }

        protected void doStartElement(String uri, String name, String qName, Attributes attrs) {
            if (in("ObjectLockConfiguration")) {
                if ("Rule".equals(name)) {
                    rule = new ObjectLockConfiguration.ObjectLockRule();
                }
            } else if (in("ObjectLockConfiguration", "Rule")) {
                if ("DefaultRetention".equals(name)) {
                    defaultRetention = new ObjectLockConfiguration.DefaultRetention();
                }
            }
        }

        @Override
        public final void endElement(String uri, String name, String qName) {
            context.removeLast();
            doEndElement(uri, name, qName);
        }

        protected void doEndElement(String uri, String name, String qName) {
            if (in("ObjectLockConfiguration")) {
                if ("ObjectLockEnabled".equals(name)) {
                    objectLockConfiguration.objectLockEnabled = getText();
                } else if ("Rule".equals(name)) {
                    objectLockConfiguration.rule = rule;
                }
            } else if (in("ObjectLockConfiguration", "Rule")) {
                if ("DefaultRetention".equals(name)) {
                    rule.defaultRetention = defaultRetention;
                }
            } else if (in("ObjectLockConfiguration", "Rule", "DefaultRetention")) {
                if ("Mode".equals(name)) {
                    defaultRetention.mode = getText();
                } else if ("Days".equals(name)) {
                    defaultRetention.days = Integer.parseInt(getText());
                } else if ("Years".equals(name)) {
                    defaultRetention.years = Integer.parseInt(getText());
                }
            }
        }

        @Override
        public final void characters(char ch[], int start, int length) {
            text.append(ch, start, length);
        }

        /**
         * @param path Path to test
         * @return True if the path provided is the same as the current context. False otherwise
         */
        protected final boolean in(String... path) {
            if (path.length != context.size()) {
                return false;
            }

            int i = 0;
            for (String element : context) {
                String pattern = path[i];
                if (!(pattern.equals("*") || pattern.equals(element))) {
                    return false;
                }
                i += 1;
            }

            return true;
        }

        protected final String getText() {
            return text.toString();
        }
    }
}
