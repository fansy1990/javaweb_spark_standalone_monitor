package demo.servlet;

import demo.util.CommonUtils;
import org.apache.commons.logging.Log;
import org.apache.spark.deploy.SparkSubmit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 完成 单个任务启动
 * 其中，Jar包需要拷贝到集群中每个slave节点对应的位置；
 * Created by fansy on 2017/11/15.
 */
public class WordCountServlet extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(WordCountServlet.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String className ="algorithm.WordCount";
        String jar =  CommonUtils.getJarPath();
        String appName = "test java web submit job to spark standalone";
        log.info(jar);
        if(CommonUtils.existResource(jar)){
            log.info("存在！");
        }else{
            log.info("不存在！");
        }
        String path = req.getParameter("path");
        log.info("path:"+path);
        String[] arg0=new String[]{
//                "--master","spark://192.168.0.75:7077",
                "--master","spark://server2.tipdm.com:6066",
                "--deploy-mode","cluster",
                "--name",appName,
                "--class",className,
                "--executor-memory","2G",
                "--total-executor-cores","10",
                "--executor-cores","2",
                path,
//                "/tmp/wc.jar",
//                "hdfs://192.168.0.74:8020/user/root/wc.jar",
//                "file:///C:/Users/fansy/tmp/tmp_spark_hdfs/target/spark_hdfs/WEB-INF/classes/jar/wc.jar",//
//                "hdfs://192.168.0.74:8020/user/root/a.txt",
                "/user/root/a.txt",
//                "hdfs://192.168.0.74:8020/tmp/"+System.currentTimeMillis()
                "/tmp/"+System.currentTimeMillis()

        };

        SparkSubmit.main(arg0);
    }
}
