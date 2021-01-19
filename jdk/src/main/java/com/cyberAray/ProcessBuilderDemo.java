package com.cyberAray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProcessBuilderDemo {

    public static void main(String[] args) throws IOException {
        runMnistSetup("aaa", "bbb", "cccc", 2);

    }

    public static void runMnistSetup( String script, String input, String output,
                              int partitions) throws IOException {
        List<String> args = new ArrayList<>();
        args.add("python");
        args.add(script);
        args.add(input);
        args.add(output);
        args.add(String.valueOf(partitions));
        // 新增args打印代码
        System.out.println(args.toString());
        // 创建操作系统进程
        ProcessBuilder builder = new ProcessBuilder(args);
        // PythonUtil.setupVirtualEnvProcess(mlContext, builder);
//        String classPath = ProcessPythonRunner.getClassPath();
//        if (classPath != null) {
//            builder.environment().put(MLConstants.CLASSPATH, classPath);
//        }
        // 创建一个新的 Process 实例
        Process child = builder.start();
        Thread inLogger = new Thread();
        Thread errLogger = new Thread();
//        inLogger.setName(mlContext.getIdentity() + "-in-logger");
        inLogger.setName("-in-logger");
        inLogger.setDaemon(true);
        errLogger.setName("-err-logger");
        errLogger.setDaemon(true);
        inLogger.start();
        errLogger.start();
        try {
            System.out.println(child.toString());
            System.out.println(child.isAlive());
            int r = child.waitFor();
            System.out.println(r);
            inLogger.join();
            errLogger.join();
            if (r != 0) {
                // 报错信息 r=1
                throw new RuntimeException("Mnist data setup returned with code " + r);
            }
        } catch (InterruptedException e) {
            //LOG.warn("Mnist data setup interrupted", e);
        } finally {
            child.destroyForcibly();
        }
    }
}
