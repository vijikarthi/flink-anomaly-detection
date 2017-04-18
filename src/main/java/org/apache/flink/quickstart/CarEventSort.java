package org.apache.flink.quickstart;


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.PriorityQueue;

public class CarEventSort {
public static void main(String[] args) throws Exception {

    // read parameters
    ParameterTool params = ParameterTool.fromArgs(args);
    //String input = params.getRequired("input");

    String input = "C:\\Vijay\\projects\\FlinkForward\\examples\\connected-car\\carOutOfOrderSmall.csv";

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // connect to the data file
    DataStream<String> carData = env.readTextFile(input);

    // map to events
    DataStream<ConnectedCarEvent> events = carData
            .map(new MapFunction<String, ConnectedCarEvent>() {
                @Override
                public ConnectedCarEvent map(String line) throws Exception {
                    ConnectedCarEvent event = ConnectedCarEvent.fromString(line);
                    System.out.println("Event Time: " + event.timestamp);
                    return event;
                }
            })
            .assignTimestampsAndWatermarks(new ConnectedCarAssigner());

    // sort events
    events.keyBy("carId")
            .process(new SortOutOfOrderEvents())
            .print();

    env.execute("Sort Connected Car Events");
}

public static class SortOutOfOrderEvents extends RichProcessFunction<ConnectedCarEvent, ConnectedCarEvent> {

    private ValueState<PriorityQueue<ConnectedCarEvent>> queueValueState = null;

    @Override
    public void open(Configuration config) {
        queueValueState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>>("", TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>(){})) {}
                        );
    }

    @Override
    public void processElement(ConnectedCarEvent connectedCarEvent, Context context, Collector<ConnectedCarEvent> collector) throws Exception {
        TimerService timerService = context.timerService();

        long contextTimestamp = context.timestamp();
        long currentProcessingTime = timerService.currentProcessingTime();
        long currentWatermarkTime = timerService.currentWatermark();


        //System.out.println("contextTimestamp: " + contextTimestamp + " currentProcessingTime: "  + currentProcessingTime + " currentWatermarkTime: " + currentWatermarkTime);

        if(contextTimestamp > currentWatermarkTime) {
            PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
            if(queue == null) {
                queue = new PriorityQueue<>(100, new Comparator<ConnectedCarEvent>() {
                    @Override
                    public int compare(ConnectedCarEvent c1, ConnectedCarEvent c2) {
                        if(c1.timestamp > c2.timestamp) {
                            return 1;
                        } else if (c1.timestamp == c2.timestamp) {
                            return 0;
                        } else {
                            return -1;
                        }
                    }
                });
            }
            queue.add(connectedCarEvent);
            queueValueState.update(queue);
            timerService.registerEventTimeTimer(connectedCarEvent.timestamp);
        }
    }

    @Override
    public void onTimer(long l, OnTimerContext onTimerContext, Collector<ConnectedCarEvent> collector) throws Exception {
        long watermarkTime = onTimerContext.timerService().currentWatermark();
        PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
        ConnectedCarEvent event = queue.peek();
        while(event != null && event.timestamp <= watermarkTime) {
            collector.collect(event);
            queue.remove(event);
            event = queue.peek();
        }
    }
}

public static class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {
    @Override
    public long extractTimestamp(ConnectedCarEvent event, long previousElementTimestamp) {
        return event.timestamp;
    }

    @Override
    public Watermark checkAndGetNextWatermark(ConnectedCarEvent event, long extractedTimestamp) {
        // simply emit a watermark with every event
        return new Watermark(extractedTimestamp - 30000);
        //return new Watermark(extractedTimestamp - 5000);
        //System.out.println("extractedTimestamp: " + extractedTimestamp);
        //return new Watermark(extractedTimestamp - 10000);
    }
}


}
