package demo.servlet;

import demo.util.CommonUtils;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.deploy.rest.SparkEngine;
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
 * 访问路径：http://localhost:8080/spark_hdfs/WordCount2?path=file:/tmp/wc.jar
 * Created by fansy on 2017/11/16.
 */
public class WordCountServlet2 extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(WordCountServlet2.class);

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

        String input = "/user/root/email_log.txt";
        String output = "/tmp/"+System.currentTimeMillis();
        String id = SparkEngine.submit(path,className,input,output);

        log.info("id:"+id);



    }


}
