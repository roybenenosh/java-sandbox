package com.example.concurrency;

import java.util.*;
import java.util.concurrent.*;

public class ProducerConsumerWithBlockingQueue {


    static final String POISON = "__POISON__";

    static List<String> runPipeline(List<String> input, int consumers, int capacity)
            throws InterruptedException {
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(capacity);
        List<String> processed = Collections.synchronizedList(new ArrayList<>());

        Thread producer = new Thread(() -> {
            try {
                for (String item : input) queue.put(item);             // blocks when full
                for (int i = 0; i < consumers; i++) queue.put(POISON); // one pill per consumer
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "producer");

        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < consumers; i++) {
            workers.add(new Thread(() -> {
                try {
                    while (true) {
                        String item = queue.take();                     // blocks when empty
                        if (item == POISON) return;                     // identity check on purpose
                        processed.add(item.toUpperCase());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "consumer-" + i));
        }

        producer.start();
        workers.forEach(Thread::start);
        producer.join();
        for (Thread w : workers) w.join();
        return processed;
    }

    static void main(String[] args) {
        try {
            // 1. Create a test input list of words
            List<String> inputWords = Arrays.asList(
                    "apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon"
            );

            System.out.println("Starting pipeline with " + inputWords.size() + " input items...");

            // 2. Run the pipeline: 3 consumer threads, queue capacity of 4
            List<String> results = runPipeline(inputWords, 3, 4);

            // 3. Print the final results
            System.out.println("\n--- Pipeline Completed Successfully ---");
            System.out.println("Processed items count: " + results.size());
            System.out.println("Results: " + results);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted.");
        }
    }
}
