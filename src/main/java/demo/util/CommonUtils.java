package demo.util;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import java.io.File;

/**
 * Created by fansy on 2017/11/15.
 */
public class CommonUtils {
    private  static final String JAR_PATH = "jar/wc.jar";

    public static String getURI(String file){
        return new File(Thread.currentThread().getContextClassLoader().getResource("").getPath() + file).toURI().toString();
    }

    public static String getJarPath(){
        return getURI(JAR_PATH);
    }

    public static boolean existResource(String filePath){
        return new File(filePath).exists();
    }
}
