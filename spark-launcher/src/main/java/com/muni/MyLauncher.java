package com.muni;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by muni on 2019/8/14
 */
public class MyLauncher {
    public static void main(String[] args) {
        String errFile = System.getProperty("user.dir") + "/logs/spark-err.log";
        String outputFile = System.getProperty("user.dir") + "/logs/spark-output.log";
        String logFile = MyLauncher.class.getClassLoader().getResource("test.txt").getFile();

        try {
            // clear log
            new PrintWriter(errFile).close();
            new PrintWriter(outputFile).close();

            SparkLauncher launcher = new SparkLauncher()
                    .addSparkArg("--conf", "spark.driver.cores=1")
                    .setAppName("Test Spark")
                    .setMaster("local")
                    .setSparkHome("/Users/muni/Downloads/spark-2.4.0-bin-hadoop2.7")
                    .setMainClass("com.muni.SparkMain")
                    .setAppResource("/Users/muni/IdeaProjects/spark-demo/spark-main/target/spark-main-1.0-SNAPSHOT.jar")
                    .redirectError(new File(errFile))
                    .redirectOutput(new File(outputFile))
                    .addAppArgs(logFile);


            SparkAppHandle handler = launcher.startApplication(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println(sparkAppHandle.getAppId() + " -> state changed: " + sparkAppHandle.getState());
                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println(sparkAppHandle.getAppId() + " -> info changed");
                }
            });

            while (!handler.getState().isFinal()) {
                System.out.println(handler.getAppId() + " -> " + handler.getState());
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
