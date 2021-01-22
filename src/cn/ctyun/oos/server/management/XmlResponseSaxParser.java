package cn.ctyun.oos.server.management;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import cn.ctyun.oos.server.db.dbprice.DbPackage;
import cn.ctyun.oos.server.db.dbprice.DbTieredPrice;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser;

/**
 * @author: Cui Meng
 */
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
    
    public PackageHandler parsePackage(InputStream inputStream) throws AmazonClientException {
        PackageHandler handler = new PackageHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
        return handler;
    }
    
    public TieredPriceHandler parseTieredPrice(InputStream inputStream)
            throws AmazonClientException {
        TieredPriceHandler handler = new TieredPriceHandler();
        parseXmlInputStream(handler, sanitizeXmlDocument(handler, inputStream));
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
                    } catch (IOException e) {
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
    
    public class PackageHandler extends DefaultHandler {
        private DbPackage dbPackage = null;
        private StringBuilder currText = null;
        
        public PackageHandler() {
            super();
            dbPackage = new DbPackage();
            this.currText = new StringBuilder();
        }
        
        /**
         * @return the buckets listed in the document.
         */
        public DbPackage getPackage() {
            return dbPackage;
        }
        
        /**
         * @return the owner of the buckets.
         */
        public void startDocument() {
        }
        
        public void endDocument() {
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            if (name.equals("ID")) {
                dbPackage.packageId = Integer.parseInt(elementText);
            } else if (name.equals("Name")) {
                dbPackage.name = elementText;
            } else if (name.equals("Storage")) {
                dbPackage.storage = Long.parseLong(elementText);
            } else if (name.equals("Flow")) {
                dbPackage.flow = Long.parseLong(elementText);
            } else if (name.equals("GHRequest")) {
                dbPackage.ghRequest = Long.parseLong(elementText);
            } else if (name.equals("OtherRequest")) {
                dbPackage.otherRequest = Long.parseLong(elementText);
            } else if (name.equals("Duration")) {
                dbPackage.duration = Long.parseLong(elementText);
            } else if (name.equals("CostPrice")) {
                dbPackage.costPrice = Double.parseDouble(elementText);
            } else if (name.equals("PackagePrice")) {
                dbPackage.packagePrice = Double.parseDouble(elementText);
            } else if (name.equals("Discount")) {
                dbPackage.discount = Double.parseDouble(elementText);
            } else if (name.equals("IsValid")) {
                dbPackage.isValid = Byte.parseByte(elementText);
            } else if (name.equals("IsVisible")) {
                dbPackage.isVisible = Byte.parseByte(elementText);
            } else if (name.equals("RoamUpload")) {
                dbPackage.roamUpload = Long.parseLong(elementText);
            } else if (name.equals("RoamFlow")) {
                dbPackage.roamFlow = Long.parseLong(elementText);
            } else if (name.equals("NoNetFlow")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.noNetFlow = Long.parseLong(elementText);
                }              
            } else if (name.equals("NoNetGHReq")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.noNetGHReq = Long.parseLong(elementText);
                }             
            } else if (name.equals("NoNetOtherReq")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.noNetOtherReq = Long.parseLong(elementText);
                }                
            } else if (name.equals("NoNetRoamFlow")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.noNetRoamFlow = Long.parseLong(elementText);
                }                
            } else if (name.equals("NoNetRoamUpload")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.noNetRoamUpload = Long.parseLong(elementText);
                }                
            } else if (name.equals("SpamRequest")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.spamRequest = Long.parseLong(elementText);
                }
            } else if (name.equals("Porn")) {
                if (elementText != null && !elementText.equals("")) {
                    dbPackage.porn = Long.parseLong(elementText);
                }
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }
    
    public class TieredPriceHandler extends DefaultHandler {
        private DbTieredPrice dbTP = null;
        private StringBuilder currText = null;
        
        public TieredPriceHandler() {
            super();
            dbTP = new DbTieredPrice();
            this.currText = new StringBuilder();
        }
        
        /**
         * @return the buckets listed in the document.
         */
        public DbTieredPrice getTieredPrice() {
            return dbTP;
        }
        
        /**
         * @return the owner of the buckets.
         */
        public void startDocument() {
        }
        
        public void endDocument() {
        }
        
        public void startElement(String uri, String name, String qName, Attributes attrs) {
        }
        
        public void endElement(String uri, String name, String qName) {
            String elementText = this.currText.toString().trim();
            if (name.equals("Type")) {
                dbTP.type = elementText;
            } else if (name.equals("PriceId")) {
                dbTP.id = Integer.parseInt(elementText);
            } else if (name.equals("Low")) {
                dbTP.low = Long.parseLong(elementText);
            } else if (name.equals("Up")) {
                dbTP.up = Long.parseLong(elementText);
            } else if (name.equals("Price")) {
                dbTP.price = Double.parseDouble(elementText);
            } else if (name.equals("Discount")) {
                dbTP.discount = Double.parseDouble(elementText);
            }
            this.currText = new StringBuilder();
        }
        
        public void characters(char ch[], int start, int length) {
            this.currText.append(ch, start, length);
        }
    }
}
