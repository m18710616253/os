package cn.ctyun.oos.server.signer;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.http.HttpMethodName;

import cn.ctyun.common.BaseException;
import cn.ctyun.common.Consts;
import cn.ctyun.oos.common.ErrorMessage;
import cn.ctyun.oos.common.OOSRequest;
import common.tuple.Pair;

public class SignerUtils {
    private static DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    private static DateFormat timeFormatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
    private static TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final char QP_SEP_A = '&';
    private static final String NAME_VALUE_SEPARATOR = "=";
    private static final BitSet URLENCODER   = new BitSet(256);
    private static final int RADIX = 16;
    private static final Locale LOCALE_ENGLISH = Locale.ENGLISH;
    private static final char CHAR_SPACE = ' ';
    private static final char CHAR_TAB = '\t';
    private static final char CHAR_NEW_LINE = '\n';
    private static final char CHAR_VERTICAL_TAB = '\u000b';
    private static final char CHAR_CARRIAGE_RETURN = '\r';
    private static final char CHAR_FORM_FEED = '\f';
    private static final Pattern ENCODED_CHARACTERS_PATTERN;
    private static final List<String> listOfHeadersToIgnoreInLowerCase = Arrays.asList("connection");
    static {
        StringBuilder pattern = new StringBuilder();
        pattern.append(Pattern.quote("+"))
            .append("|")
            .append(Pattern.quote("*"))
            .append("|")
            .append(Pattern.quote("%7E"))
            .append("|")
            .append(Pattern.quote("%2F"));
        ENCODED_CHARACTERS_PATTERN = Pattern.compile(pattern.toString());
    }

    public static String formatDateStamp(long timeMilli) {
        synchronized (dateFormatter) {
            dateFormatter.setTimeZone(UTC);
            return dateFormatter.format(new Date(timeMilli));
        }
    }
    
    public static Date formatDateStamp(String date) throws ParseException {
        synchronized (dateFormatter) {
            dateFormatter.setTimeZone(UTC);
            return dateFormatter.parse(date);
        }
    }
    
    public static String formatTimestamp(long timeMilli) {
        synchronized (timeFormatter) {
            timeFormatter.setTimeZone(UTC);
            return timeFormatter.format(new Date(timeMilli));
        }
    }
    
    public static Date formatTimestamp(String date) throws ParseException {
        synchronized (timeFormatter) {
            timeFormatter.setTimeZone(UTC);
            return timeFormatter.parse(date);
        }
    }
    
    public static Pair<String,String> getFormattedDateTimeFromHead(OOSRequest<?> request) throws BaseException {
        Map<String, String> reqHeaders = request.getHeaders();
        // 请求头key大小写不敏感
        Map<String, String> requestHeaders = SignerUtils.changeMapKeyLowercase(reqHeaders);
        String date = null;
        if (requestHeaders.containsKey(V4Signer.X_AMZ_DATE)) {
            date = requestHeaders.get(V4Signer.X_AMZ_DATE);
        } else if (requestHeaders.containsKey(V4Signer.DATE)) {
            date = requestHeaders.get(V4Signer.DATE);
        } else {
            throw new BaseException(403, ErrorMessage.ERROR_CODE_403, ErrorMessage.ERROR_MESSAGE_INVALID_DATE_HEADER);
        }
        String formattedDateTime = date;
        String formattedDate = date.split("T")[0];
        Pair<String,String> p = new Pair<String,String>();
        p.first(formattedDateTime);
        p.second(formattedDate);
        return p;
    }
    
    public static Pair<String,String> getFormattedDateTimeFromQuery(OOSRequest<?> request) throws BaseException {
        Map<String, String> requestParas = request.getParameters();
        String date = null;
        if (requestParas.containsKey(V4Signer.X_AMZ_DATE_CAPITAL)) {
            date = requestParas.get(V4Signer.X_AMZ_DATE_CAPITAL);
        } else {
            throw new BaseException(400, ErrorMessage.ERROR_CODE_AUTH_QUERY_ERROR, ErrorMessage.ERROR_MESSAGE_MISSING_AUTH_QUERY);
        }
        String formattedDateTime = date;
        String formattedDate = date.split("T")[0];
        Pair<String,String> p = new Pair<String,String>();
        p.first(formattedDateTime);
        p.second(formattedDate);
        return p;
    }
    
    public static Pair<String,String> getFormattedDateTimeFromPost(String date) {
        String formattedDateTime = date;
        String formattedDate = date.split("T")[0];
        Pair<String,String> p = new Pair<String,String>();
        p.first(formattedDateTime);
        p.second(formattedDate);
        return p;
    }
    
    /**
     * Returns true if the specified URI is using a non-standard port (i.e. any
     * port other than 80 for HTTP URIs or any port other than 443 for HTTPS
     * URIs).
     *
     * @param uri
     *
     * @return True if the specified URI is using a non-standard port, otherwise
     *         false.
     */
    public static boolean isUsingNonDefaultPort(URI uri) {
        String scheme = uri.getScheme().toLowerCase();
        int port = uri.getPort();
        if (port <= 0) return false;
        if (scheme.equals("http") && port == 80) return false;
        if (scheme.equals("https") && port == 443) return false;
        return true;
    }
    
    public static boolean usePayloadForQueryParameters(OOSRequest<?> request) {
        boolean requestIsPOST = HttpMethodName.POST.equals(request.getHttpMethod());
        boolean requestHasNoPayload = request.getContent() == null ? true : false;
        return requestIsPOST && requestHasNoPayload;
    }
    
    /**
     * Creates an encoded query string from all the parameters in the specified
     * request.
     *
     * @param request
     *            The request containing the parameters to encode.
     *
     * @return Null if no parameters were present, otherwise the encoded query
     *         string for the parameters present in the specified request.
     */
    public static String encodeParameters(OOSRequest<?> request) {
        Map<String, String> requestParams = request.getParameters();
        if (requestParams.isEmpty()) return null;
        List<Pair<String,String>> nameValuePairs = new ArrayList<Pair<String,String>>();
        for (Entry<String, String> entry : requestParams.entrySet()) {
            String parameterName = entry.getKey();
            String value = entry.getValue();
            Pair<String, String> p = new Pair<String, String>();
            p.first(parameterName);
            p.second(value);
            nameValuePairs.add(p);
        }
        return format(nameValuePairs, QP_SEP_A, "UTF-8");
    }
    
    /**
     * Returns a String that is suitable for use as an {@code application/x-www-form-urlencoded}
     * list of parameters in an HTTP PUT or HTTP POST.
     *
     * @param parameters  The parameters to include.
     * @param parameterSeparator The parameter separator, by convention, {@code '&'} or {@code ';'}.
     * @param charset The encoding to use.
     * @return An {@code application/x-www-form-urlencoded} string
     *
     * @since 4.3
     */
    public static String format(List<Pair<String,String>> parameters, char parameterSeparator, String charset) {
        StringBuilder result = new StringBuilder();
        for (Pair<String,String> parameter : parameters) {
            String encodedName = encodeFormFields(parameter.first(), charset);
            String encodedValue = encodeFormFields(parameter.second(), charset);
            if (result.length() > 0) {
                result.append(parameterSeparator);
            }
            result.append(encodedName);
            if (encodedValue != null) {
                result.append(NAME_VALUE_SEPARATOR);
                result.append(encodedValue);
            }
        }
        return result.toString();
    }
    
    public static String encodeFormFields(String content, String charset) {
        if (content == null) {
            return null;
        }
        return urlEncode(content, charset != null ? Charset.forName(charset) : Consts.CS_UTF8, URLENCODER, true);
    }
    
    public static String urlEncode(String content, Charset charset, BitSet safechars, boolean blankAsPlus) {
        if (content == null) {
            return null;
        }
        StringBuilder buf = new StringBuilder();
        ByteBuffer bb = charset.encode(content);
        while (bb.hasRemaining()) {
            int b = bb.get() & 0xff;
            if (safechars.get(b)) {
                buf.append((char) b);
            } else if (blankAsPlus && b == ' ') {
                buf.append('+');
            } else {
                buf.append("%");
                char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, RADIX));
                char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, RADIX));
                buf.append(hex1);
                buf.append(hex2);
            }
        }
        return buf.toString();
    }
    
    /**
     * Append the given path to the given baseUri.
     * By default, all slash characters in path will not be url-encoded.
     */
    public static String appendUri(String baseUri, String path) {
        return appendUri(baseUri, path, false);
    }

    /**
     * Append the given path to the given baseUri.
     *
     * @param baseUri The URI to append to (required, may be relative)
     * @param path The path to append (may be null or empty).  Path should be pre-encoded.
     * @param escapeDoubleSlash Whether double-slash in the path should be escaped to "/%2F"
     * @return The baseUri with the path appended
     */
    public static String appendUri(String baseUri, String path, boolean escapeDoubleSlash) {
        String resultUri = baseUri;
        if (path != null && path.length() > 0) {
            if (path.startsWith("/")) {
                // trim the trailing slash in baseUri, since the path already starts with a slash
                if (resultUri.endsWith("/")) {
                    resultUri = resultUri.substring(0, resultUri.length() - 1);
                }
            } else if (!resultUri.endsWith("/")) {
                resultUri += "/";
            }
            if (escapeDoubleSlash) {
                resultUri += path.replace("//", "/%2F");
            } else {
                resultUri += path;
            }
        } else if (!resultUri.endsWith("/")) {
            resultUri += "/";
        }

        return resultUri;
    }
    
    /**
     * Encode a string for use in the path of a URL; uses URLEncoder.encode,
     * (which encodes a string for use in the query portion of a URL), then
     * applies some postfilters to fix things up per the RFC. Can optionally
     * handle strings which are meant to encode a path (ie include '/'es
     * which should NOT be escaped).
     *
     * @param value the value to encode
     * @param path true if the value is intended to represent a path
     * @return the encoded value
     */
    public static String urlEncode(String value, boolean path) {
        if (value == null) {
            return "";
        }
        try {
            String encoded = URLEncoder.encode(value, Consts.STR_UTF8);
            Matcher matcher = ENCODED_CHARACTERS_PATTERN.matcher(encoded);
            StringBuffer buffer = new StringBuffer(encoded.length());
            while (matcher.find()) {
                String replacement = matcher.group(0);
                if ("+".equals(replacement)) {
                    replacement = "%20";
                } else if ("*".equals(replacement)) {
                    replacement = "%2A";
                } else if ("%7E".equals(replacement)) {
                    replacement = "~";
                } else if (path && "%2F".equals(replacement)) {
                    replacement = "/";
                }
                matcher.appendReplacement(buffer, replacement);
            }
            matcher.appendTail(buffer);
            return buffer.toString();

        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static String lowerCase(String str) {
        if(isNullOrEmpty(str)) {
            return str;
        }
        return str.toLowerCase(LOCALE_ENGLISH);
    }
    
    public static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }
    
    public static void appendCompactedString(StringBuilder destination, String source) {
        boolean previousIsWhiteSpace = false;
        int length = source.length();
        for (int i = 0; i < length; i++) {
            char ch = source.charAt(i);
            if (isWhiteSpace(ch)) {
                if (previousIsWhiteSpace) {
                    continue;
                }
                destination.append(CHAR_SPACE);
                previousIsWhiteSpace = true;
            } else {
                destination.append(ch);
                previousIsWhiteSpace = false;
            }
        }
    }
    
    public static boolean isWhiteSpace(char ch) {
        if (ch == CHAR_SPACE) return true;
        if (ch == CHAR_TAB) return true;
        if (ch == CHAR_NEW_LINE) return true;
        if (ch == CHAR_VERTICAL_TAB) return true;
        if (ch == CHAR_CARRIAGE_RETURN) return true;
        if (ch == CHAR_FORM_FEED) return true;
        return false;
    }
    
    public static boolean shouldExcludeHeaderFromSigning(String header) {
        return listOfHeadersToIgnoreInLowerCase.contains(header.toLowerCase());
    }
    
    public static String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (int i = 0; i < data.length; i++) {
            String hex = Integer.toHexString(data[i]);
            if (hex.length() == 1) {
                // Append leading zero.
                sb.append("0");
            } else if (hex.length() == 8) {
                // Remove ff prefix from negative numbers.
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase(Locale.getDefault());
    }
    
    /**
     * 将map的key转成小写形式，合并重名key的value，以逗号分隔
     * @param request
     * @return
     */
    public static Map<String, String> changeMapKeyLowercase(Map<String, String> map) {
        Map<String, String> changedMap = new HashMap<String, String>();
        for (Entry<String, String> entry : map.entrySet()) {
            String k = entry.getKey().toLowerCase();
            String v = entry.getValue();
            if (changedMap.containsKey(k)) {
              //合并重名key的value，以逗号分隔
                v = changedMap.get(k).concat(",").concat(v);
            }
            changedMap.put(k, v);
        }
        return changedMap;
    }

    public static void parseDateStamp(String dateStamp) throws ParseException {
        synchronized (dateFormatter) {
            dateFormatter.setLenient(false);
            dateFormatter.parse(dateStamp);
        }
    }

}
