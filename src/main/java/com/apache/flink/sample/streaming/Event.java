package com.apache.flink.sample.streaming;

import java.io.Serializable;

public class Event implements Serializable {

    public int sourceAddress;

    public EventType event;

    public Event() {}

    public Event(int address, EventType event) {
        this.sourceAddress = address;
        this.event = event;
    }

    @Override
    public String toString() {
        return "Event " + EventType.formatAddress(sourceAddress) + ": " + EventType.eventTypeName(event);
    }

    public static class EventType implements Serializable {

        public static final int a = 1;
        public static final int b = 2;
        public static final int c = 3;
        public static final int d = 4;
        public static final int e = 5;
        public static final int f = 6;
        public static final int g = 7;

        private int value;

        public EventType(int value) {
            this.value = value;
        }

        public int getValue() { return value; }

        public static String eventTypeName(EventType eventType) {
            char a = 'a';
            int val = (int) a + eventType.getValue() - 1;
            String retVal =  String.valueOf(Character.toChars(val));
            return retVal;
        }

        public static String formatAddress(int address) {
            int b1 = (address >>> 24) & 0xff;
            int b2 = (address >>> 16) & 0xff;
            int b3 = (address >>>  8) & 0xff;
            int b4 =  address         & 0xff;
            return  b1 + "." + b2 + "." + b3 + "." + b4;
        }

    }

    public static class Alert implements Serializable {

        private int address;
        private EventStateMachine.State state;
        private EventType transition;

        public Alert(int address, EventStateMachine.State state, EventType transition) {
            this.address = address;
            this.state = state;
            this.transition = transition;
        }

        @Override
        public String toString() {
            return "ALERT " + EventType.formatAddress(address) + ": " + state.getName() + " -> " + EventType.eventTypeName(transition);
       }
    }

}
