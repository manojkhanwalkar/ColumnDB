package logger;

/**
 * Created by mkhanwalkar on 7/17/15.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Logger {


    private static Log log = LogFactory.getLog("Container");

    public static Log getLog()
    {
        return log;
    }

}
