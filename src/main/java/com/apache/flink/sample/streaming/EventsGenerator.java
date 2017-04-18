package com.apache.flink.sample.streaming;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class EventsGenerator {

    private double errorProb = 0.0000001;

    private Random rnd = new Random();

    private Map<Integer, EventStateMachine.State> states = new LinkedHashMap<>();

    public int numActiveEntries() {
        return states.size();
    }

    public Event next(int minIp, int maxIp) {
        double p = rnd.nextDouble();

        Event returnEvent = null;

        if (p * 1000 >= states.size()) {
            // create a new state machine
            int nextIP = rnd.nextInt(maxIp - minIp) + minIp;
            //System.out.println("nextIP: " + nextIP);

            if (!states.containsKey(nextIP)) {
                Tuple2<Event.EventType, EventStateMachine.State> stateTransitionTuple = EventStateMachine.Transitions.initialState.randomTransition(rnd);
                states.put(nextIP, stateTransitionTuple.f1);
                returnEvent = new Event(nextIP, stateTransitionTuple.f0);
                //System.out.println(returnEvent);
            }
            else {
                // collision on IP address, try again
                next(minIp, maxIp);
            }
        }
        else {
            // pick an existing state machine

            // skip over some elements in the linked map, then take the next
            // update it, and insert it at the end

            int numToSkip = Math.min(20, rnd.nextInt(states.size()));
            Iterator<Map.Entry<Integer,EventStateMachine.State>> iter = states.entrySet().iterator();
            int i = 0;
            while (i < numToSkip) {
                i += 1;
                iter.next();
            };

            Map.Entry<Integer,EventStateMachine.State> entry = iter.next();
            int address = entry.getKey();
            EventStateMachine.State currentState = entry.getValue();
            iter.remove();

            if (p < errorProb) {
                Event.EventType event = currentState.randomInvalidTransition(rnd);
                returnEvent = new Event(address, event);
            }
            else {
                Tuple2<Event.EventType, EventStateMachine.State> stateTransitionTuple = currentState.randomTransition(rnd);
                if (!stateTransitionTuple.f1.terminal()) {
                    // reinsert
                    states.put(address, stateTransitionTuple.f1);
                }
                returnEvent = new Event(address, stateTransitionTuple.f0);
            }
        }

        //System.out.println("##### [" + returnEvent + "]");
        return returnEvent;
    }

    protected Optional<Event> nextInvalid() {
        Iterator<Map.Entry<Integer,EventStateMachine.State>>  iter = states.entrySet().iterator();
        if (iter.hasNext()) {
            Map.Entry<Integer,EventStateMachine.State> entry = iter.next();
            int address = entry.getKey();
            EventStateMachine.State currentState = entry.getValue();
            iter.remove();

            Event.EventType event = currentState.randomInvalidTransition(rnd);
            Event e = new Event(address, event);
            return Optional.of(e);
        }
        return Optional.empty();
    }
}
