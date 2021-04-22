package ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * 缓存线程池
 * 如果池中的线程不够,会自动增加线程
 */
public class CachedThreadPoolTest {
    public static void main(String[] args) {
        //创建一个缓存线程池
        ExecutorService pool=Executors.newCachedThreadPool();
        for(int i=0;i<10;i++){
            final int task=i;
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    for(int j=0;j<10;j++){
                        System.out.println(Thread.currentThread().getName()+"正在进行第"+task+"个任务,第"+j+"次循环");
                    }
                }
            });
        }
        System.out.println("all of ten tasks have committed");
    }
}