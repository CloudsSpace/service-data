package ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * 只有一个线程工作的线程池
 * 始终保持池中有一个线程，当一个线程死了会立即重新创建一个线程
 */
public class SingleThreadExecutorTest {
    public static void main(String[] args) {
        ExecutorService pool=Executors.newSingleThreadExecutor();
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
