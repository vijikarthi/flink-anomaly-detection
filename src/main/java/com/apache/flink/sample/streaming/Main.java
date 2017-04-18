package com.apache.flink.sample.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Main {

    public static void main(String[] args) {

        EventsGeneratorSource eventsGeneratorSource = new EventsGeneratorSource(true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(1000);

        env.addSource(eventsGeneratorSource)
                .keyBy("sourceAddress")
                .flatMap(new StateMachineMapper())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class StateMachineMapper implements FlatMapFunction<Event, Event.Alert>, Checkpointed<HashMap<Integer,EventStateMachine.State>> {

        HashMap<Integer, EventStateMachine.State> states = new HashMap<>();

        @Override
        public void flatMap(Event event, Collector<Event.Alert> collector) throws Exception {
            EventStateMachine.State state;
            if(states.containsKey(event.sourceAddress)) {
                state = states.remove(event.sourceAddress);
            } else {
                state = EventStateMachine.Transitions.initialState;
            }

            EventStateMachine.State nextState = state.transition(event.event);
            //System.out.println("event-> [" + event + "] state-> [" + state + "] nextState-> [" + nextState + "]");
            if(nextState instanceof EventStateMachine.InvalidTransition) {
                Event.Alert alert = new Event.Alert(event.sourceAddress, state, event.event);
                collector.collect(alert);
            } else if (!nextState.terminal()){
                states.put(event.sourceAddress, nextState);
            }
        }


        @Override
        public HashMap<Integer, EventStateMachine.State> snapshotState(long l, long l1) throws Exception {
            return states;
        }

        @Override
        public void restoreState(HashMap<Integer, EventStateMachine.State> stateMap) throws Exception {
            states.putAll(stateMap);
        }
    }
}
