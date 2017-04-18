package com.emc.flink.sample.streaming;

import org.apache.flink.api.java.tuple.Tuple2;
import scala.util.parsing.combinator.testing.Str;

import java.io.Serializable;
import java.util.*;

import static com.emc.flink.sample.streaming.Event.EventType;

public class EventStateMachine {

    public EventStateMachine() {
    }

    public static class Transition implements Serializable {
        private Event.EventType event;
        private State targetState;
        private float prob;

        public Transition(EventType event, State targetState, float prob) {
            this.event = event;
            this.targetState = targetState;
            this.prob = prob;
        }
    }

    public static class State implements Serializable {

        private String name;

        private List<Transition> transitions;

        public State(String name) {
            this.name = name;
        }

        public State(String name, List<Transition> transitions) {
            this.name = name;
            this.transitions = transitions;
        }

        public String getName() { return  name; }

        @Override
        public String toString() {
            return name;
        }

        protected boolean terminal() {
            return transitions == null || transitions.isEmpty();
        }

        public State transition(final EventType eventType) {
            Optional<State> optionalState = transitions
                    .stream()
                    .filter(t -> t.event.getValue() == eventType.getValue())
                    .map(m -> m.targetState)
                    .findFirst();

            if (optionalState.isPresent()) {
                return optionalState.get();
            } else {
                return new InvalidTransition("Invalid Transition");
            }
        }

        public Tuple2<EventType, State> randomTransition(Random rnd) {
            if (transitions.isEmpty()) {
                throw new RuntimeException("Cannot transition from state " + name);
            }
            float p = rnd.nextFloat();
            float mass = 0.0f;
            Transition transition = null;

            for (Transition t : transitions) {
                mass += t.prob;
                if (transition == null && p <= mass) {
                    transition = t;
                }
            }
            return new Tuple2<>(transition.event, transition.targetState);
        }

        public Event.EventType randomInvalidTransition(Random rnd) {
            EventType value = new EventType(-1);

            while (value.getValue() == -1) {
                EventType g = new EventType(EventType.g);
                int candidate = rnd.nextInt(g.getValue() + 1);
                value = (transition(new EventType(candidate)) instanceof InvalidTransition) ? new EventType(candidate) : new EventType(-1);
            }
            return value;
        }

    }

    public static class InvalidTransition extends State {
        public InvalidTransition(String name) {
            super(name);
        }

        public InvalidTransition(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class TerminalState extends State {
        public TerminalState(String name) {
            super(name);
        }

        public TerminalState(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class W extends State {
        public W(String name) {
            super(name);
        }

        public W(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class X extends State {
        public X(String name) {
            super(name);
        }

        public X(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class Y extends State {
        public Y(String name) {
            super(name);
        }

        public Y(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class Z extends State {
        public Z(String name) {
            super(name);
        }

        public Z(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class InitialState extends State {
        public InitialState(String name) {
            super(name);
        }

        public InitialState(String name, List<Transition> transitions) {
            super(name, transitions);
        }
    }

    public static class Transitions {
        protected static Transition transitionG = new Transition(new EventType(EventType.g), new TerminalState("Terminal"), 1.0f);
        protected static Z z = new Z("Z", Arrays.asList(transitionG));

        protected static Transition transitionZ = new Transition(new EventType(EventType.e), z, 1.0f);
        protected static Y y = new Y("Y", Arrays.asList(transitionZ));

        protected static Transition transitionY = new Transition(new EventType(EventType.b), y, 1.0f);
        protected static X x = new X("X", Arrays.asList(transitionY, transitionZ));

        protected static W w = new W("W", Arrays.asList(transitionY));

        protected static Transition transitionW = new Transition(new EventType(EventType.a), w, 0.6f);
        protected static Transition transitionX = new Transition(new EventType(EventType.c), x, 0.4f);
        protected static InitialState initialState = new InitialState("Initial", Arrays.asList(transitionW, transitionX));
    }

}
