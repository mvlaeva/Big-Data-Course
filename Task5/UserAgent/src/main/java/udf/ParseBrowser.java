package udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

import eu.bitwalker.useragentutils.UserAgent;

public class ParseBrowser extends GenericUDF {

    private ObjectInspector[] argumentOIs;
    private ReturnObjectInspectorResolver returnOIResolver;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        returnOIResolver.update(arguments[0]);
        return returnOIResolver.get();
    }

    @Override
    public Text evaluate(DeferredObject[] arguments) throws HiveException {
        Object userAgent = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
        Object stringOI = returnOIResolver.convertIfNecessary(arguments[1].get(), argumentOIs[1]);
        Text userAgentText = (Text) userAgent;
        Text whatToParse = (Text) stringOI;
        UserAgent ua = UserAgent.parseUserAgentString(userAgentText.toString());
        String result;

        switch (whatToParse.toString()) {
            case "browser":
                result = ua.getBrowser().getName();
                break;
            case "os":
                result = ua.getOperatingSystem().getName();
                break;
            case "device":
                result = ua.getOperatingSystem().getDeviceType().toString();
                break;
            default:
                result = "UNKNOWN-UNKNOWN";
        }

        return new Text(result);
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}