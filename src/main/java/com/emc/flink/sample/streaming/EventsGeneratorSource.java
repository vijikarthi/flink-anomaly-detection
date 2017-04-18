package com.emc.flink.sample.streaming;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class EventsGeneratorSource extends RichParallelSourceFunction<Event> {

    protected int count = 0;
    protected boolean running = true;
    private boolean printSpeed = false;

    public EventsGeneratorSource(boolean printSpeed) {
        this.printSpeed = printSpeed;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        if(printSpeed) { printSpeed(); }

        EventsGenerator generator = new EventsGenerator();

        int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
        int min = range * getRuntimeContext().getIndexOfThisSubtask();
        int max = min + range;

        //System.out.println("Min: " + min + " Max: " + max + " Range: " + range);

        while (running) {
            Event event = generator.next(min, max);
            if(event != null) {
                sourceContext.collect(event);
                count += 1;
                //Thread.sleep(3000);
            }
        }

        // set running to false to stop the logger
        running = false;

    }

    @Override
    public void cancel() {
        running = false;
    }

    private void printSpeed() {
        Thread logger = new Thread("Throughput Logger") {
            @Override
            public void run() {
                int lastCount = 0;
                long lastTimeStamp = System.currentTimeMillis();

                while (running) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    long ts = System.currentTimeMillis();
                    int currCount = count;
                    double factor = (ts - lastTimeStamp) / 1000;
                    double perSec = (currCount - lastCount) / factor;
                    lastTimeStamp = ts;
                    lastCount = currCount;

                    System.out.println(perSec + " / sec");
                }
            }
        };
        logger.setDaemon(true);
        logger.start();
    }

}
