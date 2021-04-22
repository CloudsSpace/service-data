package ThreadPool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * 定时器线程池
 *
 */
public class ScheduledThreadPoolTest {
    public static void main(String[] args) {
//      method1();
        method2();
    }

    /**
     * 2秒之后执行线程
     */
    public static void method1(){
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(3);
        //2秒之后执行这个线程
        pool.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println("爆炸");
            }
        }, 2, TimeUnit.SECONDS);
    }

    /**
     * 5秒后第一次执行线程，之后每隔2秒执行一次
     * 也就是5秒后打印第一次爆炸，之后每隔2秒打印一次爆炸
     */
    public static void method2(){
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(3);
        pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("爆炸");
            }
        }, 5, 2, TimeUnit.SECONDS);
    }
}