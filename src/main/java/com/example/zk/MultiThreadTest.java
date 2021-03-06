package com.example.zk;

/**
 * 多线程中的锁控制
 */
public class MultiThreadTest {

    // 以一个静态变量来模拟公共资源
    private static int counter = 0;

    // 多线程环境下，会出现并发问题 TODO  synchronized
    public static void plus() {

        // 计数器加一
        counter++;

        // 线程随机休眠数毫秒，模拟现实中的耗时操作
        int sleepMillis = (int) (Math.random() * 100);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        // 开启五个线程
        CountPlus threadA = new CountPlus("threadA");
        threadA.start();

        CountPlus threadB = new CountPlus("threadB");
        threadB.start();

        CountPlus threadC = new CountPlus("threadC");
        threadC.start();

        CountPlus threadD = new CountPlus("threadD");
        threadD.start();

        CountPlus threadE = new CountPlus("threadE");
        threadE.start();
    }

    // 线程实现类
    static class CountPlus extends Thread {
        public CountPlus(String threadName) {
            super(threadName);
        }

        @Override
        public void run() {
            for (int i = 0; i < 20; i++) {
                plus();
            }
            System.out.println(Thread.currentThread().getName() + "执行完毕：" + counter);
        }

    }
}

