package org.apache.spark.deploy.rest;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

/**
 * Spark 引擎：
 * 1）调用Spark算法,提交任务到Spark StandAlone集群，并返回id；
 * 2）根据id监控Spark 任务；
 * Created by fansy on 2017/11/16.
 */
public class SparkEngine {

    private static final String MASTER="spark://server2.tipdm.com:6066";
    private static final String APPNAME="wordcount 2";

    public static String submit(String appResource,String mainClass,String ...args){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(MASTER);
        sparkConf.setAppName(APPNAME);
        sparkConf.set("spark.executor.cores","2");
        sparkConf.set("spark.submit.deployMode","cluster");
        sparkConf.set("spark.jars",appResource);
        sparkConf.set("spark.executor.memory","2G");
        sparkConf.set("spark.cores.max","10");
        sparkConf.set("spark.driver.supervise","false");


        Map<String,String> env = filterSystemEnvironment(System.getenv());

        CreateSubmissionResponse response = null;
        try {
            response = (CreateSubmissionResponse)
                    RestSubmissionClient.run(appResource, mainClass, args, sparkConf, toScalaMap(env));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return response.submissionId();
    }

    private static Map<String, String> filterSystemEnvironment(Map<String, String> env) {
        Map<String,String> map = new HashMap<>();
        for(Map.Entry<String,String> kv : env.entrySet()){
            if(kv.getKey().startsWith("SPARK_") && kv.getKey() != "SPARK_ENV_LOADED"
                    || kv.getKey().startsWith("MESOS_")){
                map.put(kv.getKey(),kv.getValue());
            }
        }
        return map;
    }

    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                Predef.<Tuple2<A, B>>conforms()
        );
    }
}
