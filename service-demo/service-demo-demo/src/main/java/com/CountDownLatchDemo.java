//package com;
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.Executors;
//
//public class CountDownLatchDemo {
//    protected static ExecutorService scheduler = Executors.newFixedThreadPool(5);
//    public static CountDownLatch countDownLatch;//定义一个静态的CountDownLatch
//    public static void main(String[] args) {
//        countDownLatch = new CountDownLatch(5); //初始化容量，
//        int threadNum = taskNum>8?8:taskNum;  //创建八个线程，和cpu数目相等
//        for(int i=0;i<taskNum;i++){
//            scheduler.execute(new MultiThread());  //启动多个线程
//        }
//        try {
//            countDownLatch.await();   //等待子线程全部执行结束（等待CountDownLatch计数变为0）
//            logger.info("main thread exit");
//            System.exit(0);//程序退出
//        } catch (InterruptedException e) {
//            System.exit(1);
//            e.printStackTrace();
//        }
//    }
//}
