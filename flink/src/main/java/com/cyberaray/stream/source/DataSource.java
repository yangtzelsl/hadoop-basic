package com.cyberaray.stream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        Random random = new Random();
        while (isRunning) {
            Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
            String key = "类别" + (char) ('A' + random.nextInt(3));
            int value = random.nextInt(10) + 1;

            System.out.println(String.format("Emits\t(%s, %d)", key, value));
            ctx.collect(new Tuple2<>(key, value));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
