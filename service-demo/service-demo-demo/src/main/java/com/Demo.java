package com;

import java.util.ArrayList;

public class Demo {
    public static void main(String[] args) throws InterruptedException {
        //测试ArrayList是否是线程安全的，开两个线程，分别往list中添加0-9和10-19
        Demo tt = new Demo();
        ArrayList<Integer> list = new ArrayList<>();

        //启动线程1
        new Thread(new Runnable() {
            public void run() {
                tt.method1(list);
            }

            ;
        }).start();

        //启动线程2
        new Thread(new Runnable() {
            public void run() {
                tt.method2(list);
            };
        }).start();

        Thread.sleep(1000);
        // 打印所有结果
        for (int i = 0; i < list.size(); i++) {
            System.out.println("第" + (i) + "个元素为：" + list.get(i));
        }
    }

    //线程1：在list中添加0-9
    public void method1(ArrayList<Integer> list) {
        for (int i = 0; i < 10; i++) {
            list.add(i);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    //线程2：在list中添加10-19
    public void method2(ArrayList<Integer> list) {
        for (int i = 10; i < 20; i++) {
            list.add(i);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
