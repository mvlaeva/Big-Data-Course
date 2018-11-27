import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import udf.ParseBrowser;

public class TestUDF {

    @Test
    public void testComplexUDFReturnsCorrectValues() throws HiveException {
        ParseBrowser udf = new ParseBrowser();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector whatToParse = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector[] arguments = {stringOI, whatToParse};

        udf.initialize(arguments);
        Text ua = new Text("Mozilla/5.0 (compatible; MSIE9.0;\\Windows NT 6.1; WOW64; Trident/5.0)");
        Text result = (Text) udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(ua), new GenericUDF.DeferredJavaObject(new Text("browser"))});
        Assert.assertEquals("Internet Explorer", result.toString());
    }
}

