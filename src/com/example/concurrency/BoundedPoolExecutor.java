package com.example.concurrency;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundedPoolExecutor {

    static ThreadPoolExecutor boundedIoPool(String name, int threads, int queueCapacity) {
        AtomicInteger counter = new AtomicInteger();

        ThreadFactory factory = r -> {
            Thread t = new Thread(r, name + "-" + counter.incrementAndGet()); // readable thread dumps
            t.setUncaughtExceptionHandler((th, ex) -> System.err.println(th.getName() + " failed: " + ex));
            return t;
        };

        return new ThreadPoolExecutor(
                threads, threads, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),   // bounded: memory is capped
                factory,
                new ThreadPoolExecutor.CallerRunsPolicy()  // queue full -> submitter runs task -> slows producer
        );
    }

    static void shutdownGracefully(ExecutorService pool, long waitSeconds) {
        pool.shutdown();                                   // no new tasks, finish queued ones
        try {
            if (!pool.awaitTermination(waitSeconds, TimeUnit.SECONDS)) {
                List<Runnable> dropped = pool.shutdownNow(); // interrupt running, return queued
                System.err.println("Forced shutdown, dropped " + dropped.size());
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();            // never swallow an interrupt
        }
    }

    static void main(String[] args) {
        ThreadPoolExecutor pool = boundedIoPool("io-worker", 4, 10);

        int totalTasks = 100;
        long startTime = System.currentTimeMillis();

        System.out.println("Submitting " + totalTasks + " tasks...");

        for (int i = 1; i <= totalTasks; i++) {
            final int taskId = i;
            pool.submit(() -> {
                try {
                    Thread.sleep(20); // simulated 20 ms work
                    String currentThread = Thread.currentThread().getName();

                    // Highlight if the main (submitting) thread was forced to execute the task
                    if (currentThread.equals("main")) {
                        System.out.println("Task " + taskId + " ran on CALLER thread (" + currentThread + ")");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Wait for everything to finish
        shutdownGracefully(pool, 5);

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("==========================================");
        System.out.println("Completed " + totalTasks + " tasks in " + duration + " ms");
        System.out.println("==========================================");
    }
}

