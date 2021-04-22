package ThreadPool;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 固定线程池
 * 使用Executors.newFixedThreadPool(3)方法指定池中允许执行线程的个数
 * 如果超过这个线程个数,后面的线程就会等待
 */
public class FixedThreadPoolTest {
    public static void main(String[] args) {
        //创建一个初始线程为3个的线程池
        ExecutorService pool = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 10; i++) {
            final int task = i;
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    for (int j = 0; j < 10; j++) {
                        System.out.println(Thread.currentThread().getName() + "正在进行第" + task + "个任务,第" + j + "次循环");
                    }
                }
            });
        }
        System.out.println("all of ten tasks have committed");

        //关闭线程池
        pool.shutdown();
    }
}