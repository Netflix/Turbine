/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;

/**
 * A number which can be used to track counters (increment) or set values over time.
 * <p>
 * It is "rolling" in the sense that a 'timeInMilliseconds' is given that you want to track (such as 10 seconds) and then that is broken into buckets (defaults to 10) so that the 10 second window doesn't empty out and restart every 10 seconds, but
 * instead every 1 second you have a new bucket added and one dropped so that 9 of the buckets remain and only the newest starts from scratch.
 * <p>
 * This is done so that the statistics are gathered over a rolling 10 second window with data being added/dropped in 1 seconds intervals (or whatever granularity is defined by the arguments) rather than each 10 second window starting at 0 again.
 * <p>
 * See inner-class UnitTest for usage and expected behavior examples.
 */
public class StatsRollingNumber {

    private static final Logger logger = LoggerFactory.getLogger(StatsRollingNumber.class);

    public static enum Type {
        MAX_RATE, MIN_RATE, EVENT_DISCARDED, EVENT_PROCESSED
    }
    
    // the defaults that can be re-used by all StatsRollingNumber objects. Using this technique avoids re-creating a bunch of fast property objects
    private static final int DEFAULT_TIME_IN_MILLIS = 10000;
    private static final int DEFAULT_BUCKETS = 10;
    
    private static final DynamicIntProperty defaultTimeInMilliseconds = DynamicPropertyFactory.getInstance().getIntProperty("turbine.StatsRollingNumber.defaultTimeInMilliseconds", DEFAULT_TIME_IN_MILLIS);
    private static final DynamicIntProperty defaultNumberOfBuckets = DynamicPropertyFactory.getInstance().getIntProperty("turbine.StatsRollingNumber.defaultBuckets", DEFAULT_BUCKETS);

    private final DynamicIntProperty timeInMilliseconds;
    private final DynamicIntProperty numberOfBuckets;

    private final BucketCircularArray buckets;

    public StatsRollingNumber(DynamicIntProperty timeInMilliseconds) {
        this(timeInMilliseconds, defaultNumberOfBuckets);
    }

    public StatsRollingNumber(int timeInMilliseconds, int numberOfBuckets) {
        this( (timeInMilliseconds == DEFAULT_TIME_IN_MILLIS ? defaultTimeInMilliseconds : DynamicPropertyFactory.getInstance().getIntProperty("turbine.StatsRollingNumber.defaultTimeInMilliseconds", timeInMilliseconds)),
                (numberOfBuckets == DEFAULT_BUCKETS ? defaultNumberOfBuckets : DynamicPropertyFactory.getInstance().getIntProperty("turbine.StatsRollingNumber.defaultBuckets", numberOfBuckets)));
    }

    public StatsRollingNumber(DynamicIntProperty timeInMilliseconds, DynamicIntProperty numberOfBuckets) {
        
        this.timeInMilliseconds = timeInMilliseconds;
        this.numberOfBuckets = numberOfBuckets;

        if (timeInMilliseconds.get() % numberOfBuckets.get() != 0) {
            throw new IllegalArgumentException("The timeInMilliseconds must divide equally into numberOfBuckets. For example 1000/10 is ok, 1000/11 is not.");
        }

        buckets = new BucketCircularArray(numberOfBuckets.get());
    }

    private int getBucketSizeInMilliseconds() {
        return timeInMilliseconds.get() / numberOfBuckets.get();
    }

    public int getNumberOfBuckets() {
        return numberOfBuckets.get();
    }

    public int getRollingTimeInMilliseconds() {
        return timeInMilliseconds.get();
    }

    public void increment(Type type) {
        Bucket b = getCurrentBucket();
        b.get(type).incrementAndGet();
        b.flipModified(type);
    }

    public void increment(Type type, int delta) {
        Bucket b = getCurrentBucket();
        b.get(type).addAndGet(delta);
        b.flipModified(type);
    }

    /**
     * A number in a rolling bucket that you can set.
     * 
     * @param type
     * @param value
     */
    public void set(Type type, int value) {
        Bucket b = getCurrentBucket();
        b.get(type).set(value);
        b.flipModified(type);
    }
    
    /**
     * A compareAndSet counterpart to <code>set</code> to allow atomically safe updates of an existing value.
     * <p>
     * Will set value to 'value' if 'current value' == 'expected'
     * 
     * @param type
     * @param expected
     * @param value
     * @return true if successful, false means the current actual value was not equal to expected value.
     */
    public boolean compareAndSet(Type type, int expected, int value) {
        return getCurrentBucket().get(type).compareAndSet(expected, value);
    }

    /**
     * Force a reset of all counters (clear all buckets) so that statistics start being gathered from scratch.
     */
    public void reset() {
        buckets.clear();
    }

    /**
     * Get the sum of all buckets in the rolling counter for the given CounterType.
     * 
     * @param type
     * @return int
     */
    public int getCount(Type type) {
        return getSum(type);
    }

    /**
     * Get the sum of all buckets in the rolling counter for the given CounterType.
     * 
     * @param type
     * @return int
     */
    public int getSum(Type type) {
        Bucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return 0;

        int sum = 0;
        for (Bucket b : buckets) {
            sum += b.get(type).get();
        }
        return sum;
    }

    /**
     * Get the sum of all buckets in the rolling counter for the given CounterType.
     * 
     * @param type
     * @return int
     */
    public int getValueOfLatestBucket(Type type) {
        Bucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return 0;
        // we have bucket data so we'll return the lastBucket
        return lastBucket.get(type).get();
    }

    /**
     * Get an array of values for all buckets in the rolling counter for the given CounterType.
     * <p>
     * Index 0 is the oldest bucket.
     * 
     * @param type
     * @return int[]
     */
    public int[] getValues(Type type) {
        Bucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return new int[0];

        // we have bucket data so we'll return an array of values for all buckets
        int values[] = new int[buckets.size()];
        int i = 0;
        for (Bucket bucket : buckets) {
            values[i++] = bucket.get(type).get();
        }
        return values;
    }

    public int[] getModifiedValues(Type type) {
        Bucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return new int[0];

        // we have bucket data so we'll return an array of values for all buckets
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (Bucket bucket : buckets) {
            if (bucket.isModified(type)) {
                list.add(bucket.get(type).get());
            }
        }
        int[] arr = new int[list.size()];
        int i=0;
        for (Integer a : list) {
            arr[i++] = a;
        }
        return arr;
    }

    /**
     * Get the max value of values in all buckets for the given CounterType.
     * 
     * @param type
     * @return int
     */
    public int getMaxValue(Type type) {
        int values[] = getValues(type);
        if (values.length == 0) {
            return 0;
        } else {
            Arrays.sort(values);
            return values[values.length - 1];
        }
    }

    /**
     * Get the min value of values in all buckets for the given CounterType.
     * 
     * @param type
     * @return int
     */
    public int getMinValue(Type type) {
        int values[] = getValues(type);
        if (values.length == 0) {
            return 0;
        } else {
            Arrays.sort(values);
            return values[0];
        }
    }

    private ReentrantLock newBucketLock = new ReentrantLock();

    private Bucket getCurrentBucket() {
        long currentTime = System.currentTimeMillis();

        /* a shortcut to try and get the most common result of immediately finding the current bucket */

        /**
         * Retrieve the latest bucket if the given time is BEFORE the end of the bucket window, otherwise it returns NULL.
         * 
         * NOTE: This is thread-safe because it's accessing 'buckets' which is a LinkedBlockingDeque
         */
        Bucket currentBucket = buckets.peekLast();
        if (currentBucket != null && currentTime < currentBucket.windowStart + getBucketSizeInMilliseconds()) {
            // if we're within the bucket 'window of time' return the current one
            // NOTE: We do not worry if we are BEFORE the window in a weird case of where thread scheduling causes that to occur,
            // we'll just use the latest as long as we're not AFTER the window
            return currentBucket;
        }

        /* if we didn't find the current bucket above, then we have to create one */

        /**
         * The following needs to be synchronized/locked even with a synchronized/thread-safe data structure such as LinkedBlockingDeque because
         * the logic involves multiple steps to check existence, create an object then insert the object. The 'check' or 'insertion' themselves
         * are thread-safe by themselves but not the aggregate algorithm, thus we put this entire block of logic inside synchronized.
         * 
         * I am using a tryLock if/then (http://download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/Lock.html#tryLock())
         * so that a single thread will get the lock and as soon as one thread gets the lock all others will go the 'else' block
         * and just return the currentBucket until the newBucket is created. This should allow the throughput to be far higher
         * and only slow down 1 thread instead of blocking all of them in each cycle of creating a new bucket based on some testing
         * (and it makes sense that it should as well).
         * 
         * This means the timing won't be exact to the millisecond as to what data ends up in a bucket, but that's acceptable.
         * It's not critical to have exact precision to the millisecond, as long as it's rolling, if we can instead reduce the impact synchronization.
         * 
         * More importantly though it means that the 'if' block within the lock needs to be careful about what it changes that can still
         * be accessed concurrently in the 'else' block since we're not completely synchronizing access.
         * 
         * For example, we can't have a multi-step process to add a bucket, remove a bucket, then update the sum since the 'else' block of code
         * can retrieve the sum while this is all happening. The trade-off is that we don't maintain the rolling sum and let readers just iterate
         * bucket to calculate the sum themselves. This is an example of favoring write-performance instead of read-performance and how the tryLock
         * versus a synchronized block needs to be accommodated.
         */
        if (newBucketLock.tryLock()) {
            try {
                if (buckets.peekLast() == null) {
                    // the list is empty so create the first bucket
                    Bucket newBucket = new Bucket(currentTime);
                    buckets.addLast(newBucket);
                    return newBucket;
                } else {
                    // We go into a loop so that it will create as many buckets as needed to catch up to the current time
                    // as we want the buckets complete even if we don't have transactions during a period of time.
                    for (int i = 0; i < numberOfBuckets.get(); i++) {
                        // we have at least 1 bucket so retrieve it
                        Bucket lastBucket = buckets.peekLast();
                        if (currentTime < lastBucket.windowStart + getBucketSizeInMilliseconds()) {
                            // if we're within the bucket 'window of time' return the current one
                            // NOTE: We do not worry if we are BEFORE the window in a weird case of where thread scheduling causes that to occur,
                            // we'll just use the latest as long as we're not AFTER the window
                            return lastBucket;
                        } else if (currentTime - (lastBucket.windowStart + getBucketSizeInMilliseconds()) > timeInMilliseconds.get()) {
                            // the time passed is greater than the entire rolling counter so we want to clear it all and start from scratch
                            reset();
                            // recursively call getCurrentBucket which will create a new bucket and return it
                            return getCurrentBucket();
                        } else { // we're past the window so we need to create a new bucket
                            // create a new bucket and add it as the new 'last'
                            buckets.addLast(new Bucket(lastBucket.windowStart + getBucketSizeInMilliseconds()));
                            if (buckets.size() > numberOfBuckets.get()) {
                                // if the linkedlist 'stack' is larger than 'numberOfBuckets' drop the head to get it back to the correct size
                                buckets.removeFirst();
                            }
                        }
                    }
                    // we have finished the for-loop and created all of the buckets, so return the lastBucket now
                    return buckets.peekLast();
                }
            } finally {
                newBucketLock.unlock();
            }
        } else {
            if (buckets.peekLast() != null) {
                // we didn't get the lock so just return the latest bucket while another thread creates the next one
                return buckets.peekLast();
            } else {
                // the rare scenario where multiple threads raced to create the very first bucket
                // wait slightly and then use recursion while the other thread finishes creating a bucket
                try {
                    Thread.sleep(5);
                } catch (Exception e) {
                    // ignore
                }
                return getCurrentBucket();
            }
        }
    }

    /**
     * Counters for a given 'bucket' of time.
     */
    private static class Bucket {
        final long windowStart;
        final AtomicInteger[] atomicIntegerForCounterType;
        final AtomicBoolean[] isModified;
        
        Bucket(long startTime) {
            this.windowStart = startTime;

            // initialize the array of AtomicIntegers
            atomicIntegerForCounterType = new AtomicInteger[Type.values().length];
            isModified = new AtomicBoolean[Type.values().length];
            for (Type type : Type.values()) {
                atomicIntegerForCounterType[type.ordinal()] = new AtomicInteger();
                isModified[type.ordinal()] = new AtomicBoolean(false);
            }
        }

        AtomicInteger get(Type type) {
            return atomicIntegerForCounterType[type.ordinal()];
        }
        
        void flipModified(Type type) {
            AtomicBoolean modified = isModified[type.ordinal()];
            if (!modified.get()) {
                modified.compareAndSet(false, true);
            }
        }
        boolean isModified(Type type) {
            return isModified[type.ordinal()].get();
        }
    }

    /**
     * This is a circular array acting as a FIFO queue.
     * <p>
     * It purposefully does NOT implement Deque or some other Collection interface as it only implements functionality necessary for this RollingNumber use case.
     * <p>
     * Important Thread-Safety Note: This is ONLY thread-safe within the context of RollingNumber and the protection it gives in the <code>getCurrentBucket</code> method. It uses AtomicReference objects to ensure anything done outside of
     * <code>getCurrentBucket</code> is thread-safe, and to ensure visibility of changes across threads (ie. volatility) but the addLast and removeFirst methods are NOT thread-safe for external access they depend upon the lock.tryLock() protection in
     * <code>getCurrentBucket</code> which ensures only a single thread will access them at at time.
     */
    private class BucketCircularArray implements Iterable<Bucket> {

        private final AtomicReferenceArray<Bucket> data;
        private final AtomicReference<ListState> state;
        private final int length; // we don't resize, we always stay the same, so remember this

        /**
         * Immutable object that is atomically set every time the state of the BucketCircularArray changes
         */
        private class ListState {
            private final int size;
            private final int tail;
            private final int head;

            public ListState(int head, int tail) {
                this.head = head;
                this.tail = tail;
                if (head == 0 && tail == 0) {
                    size = 0;
                } else {
                    this.size = (tail + length - head) % length;
                }
            }

            public ListState incrementTail() {
                return new ListState(head, (tail + 1) % length);
            }

            public ListState incrementHead() {
                return new ListState((head + 1) % length, tail);
            }
        }

        BucketCircularArray(int size) {
            data = new AtomicReferenceArray<Bucket>(size + 5); // we want some extra space so a user can add then remove to keep within their defined 'size'
            state = new AtomicReference<ListState>(new ListState(0, 0));
            length = data.length();
        }

        public void clear() {
            state.set(new ListState(0, 0));
            /*
             * We are going to ignore the values in 'data' and leave them populated so we don't have to perform a second operation here that requires synchronization;
             * <p>
             * With the head/tail set to 0,0, it's as if the data doesn't exist so it won't be retrieved.
             * <p>
             * The memory burden of retaining the buckets is small and it will be GCd if the entire RollingNumber is dereferenced.
             */
        }

        public int size() {
            // the size can also be worked out each time as:
            // return (tail + data.length() - head) % data.length();
            return state.get().size;
        }

        public Bucket peekLast() {
            if (state.get().size == 0) {
                return null;
            } else {
                // we want to get the last item, so size()-1
                return data.get(convert(size() - 1, state.get()));
            }
        }

        /**
         * Returns an iterator on a copy of the internal array so that the iterator won't fail by buckets being added/removed concurrently.
         */
        public Iterator<Bucket> iterator() {
            return Collections.unmodifiableList(Arrays.asList(getArray())).iterator();
        }

        /**
         * WARNING: This is NOT truly thread-safe if this were used generically. We are however thread-safe for the limited
         * use-case of RollingNumber since addLast will ONLY be called by a single thread at a time due to protection provided in <code>getCurrentBucket</code>.
         */
        public void addLast(Bucket o) {
            // set the data value first ... it won't be visible yet because tail is not increased
            ListState currentState = state.get();
            data.set(currentState.tail, o);
            // create new version of state (what we want it to become)
            ListState newState = currentState.incrementTail();
            // another thread can race us and call 'data.set' for the same place, so we use state.compareAndSet to determine which one wins
            if (state.compareAndSet(currentState, newState)) {
                // let's see if another thread was racing us
                if (data.compareAndSet(currentState.tail, o, o)) {
                    return;
                } else {
                    // we shouldn't have more than 1 concurrent thread calling addLast
                    logger.warn("Lost a thread-race ... this method should not be called concurrently.");
                    /*
                     * I am doing logger.warn instead of throwing an exception because I feel it's not worth causing the entire
                     * app to blow up if I do have a concurrency bug since the worst that will happen is the math ends up slightly wrong.
                     * I'd rather have wrong math on counters than have exceptions prevent real transactions.
                     */
                }
            } else {
                // we shouldn't have more than 1 concurrent thread calling addLast
                logger.warn("Lost a thread-race ... this method should not be called concurrently.");
                /*
                 * I am doing logger.warn instead of throwing an exception because I feel it's not worth causing the entire
                 * app to blow up if I do have a concurrency bug since the worst that will happen is the math ends up slightly wrong.
                 * I'd rather have wrong math on counters than have exceptions prevent real transactions.
                 */
            }
        }

        /**
         * WARNING: This is NOT truly thread-safe if this were used generically. We are however thread-safe for the limited
         * use-case of RollingNumber since addLast will ONLY be called by a single thread at a time due to protection provided in <code>getCurrentBucket</code>.
         */
        public Bucket removeFirst() {
            // set the data value first ... it won't be visible yet because tail is not increased
            ListState currentState = state.get();
            Bucket toRemove = data.get(currentState.head);
            // create new version of state (what we want it to become)
            ListState newState = currentState.incrementHead();
            // another thread can race us and call 'data.set' for the same place, so we use state.compareAndSet to determine which one wins
            if (state.compareAndSet(currentState, newState)) {
                // we can just leave the data in that bucket because it's 'invisible' as long as head/tail values have changed
                return toRemove;
            } else {
                // we shouldn't have more than 1 concurrent thread calling removeFirst for the same reason that addLast should not be called concurrently
                logger.warn("Lost a thread-race ... this method should not be called concurrently.");
                /*
                 * I am doing logger.warn instead of throwing an exception because I feel it's not worth causing the entire
                 * app to blow up if I do have a concurrency bug since the worst that will happen is the math ends up slightly wrong.
                 * I'd rather have wrong math on counters than have exceptions prevent real transactions.
                 */
                return toRemove;
            }
        }

        public Bucket getLast() {
            return peekLast();
        }

        private Bucket[] getArray() {
            ArrayList<Bucket> array = new ArrayList<Bucket>();
            ListState lState = state.get();
            for (int i = 0; i < lState.size; i++) {
                array.add(data.get(convert(i, lState)));
            }
            return array.toArray(new Bucket[array.size()]);
        }

        // The convert() method takes a logical index (as if head was
        // always 0) and calculates the index within elementData
        private int convert(int index, ListState state) {
            return (index + state.head) % length;
        }

    }

    public static class UnitTest {

        @Test
        public void testCreatesBuckets() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);
                // confirm the initial settings
                assertEquals(200, counter.timeInMilliseconds.get());
                assertEquals(10, counter.numberOfBuckets.get());
                assertEquals(20, counter.getBucketSizeInMilliseconds());

                // we start out with 0 buckets in the queue
                assertEquals(0, counter.buckets.size());

                // add a success in each interval which should result in all 10 buckets being created with 1 success in each
                for (int i = 0; i < counter.numberOfBuckets.get(); i++) {
                    counter.increment(Type.MAX_RATE);
                    Thread.sleep(counter.getBucketSizeInMilliseconds());
                }

                // confirm we have all 10 buckets
                assertEquals(10, counter.buckets.size());

                // add 1 more and we should still only have 10 buckets since that's the max
                counter.increment(Type.MAX_RATE);
                assertEquals(10, counter.buckets.size());

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testResetBuckets() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // we start out with 0 buckets in the queue
                assertEquals(0, counter.buckets.size());

                // add 1
                counter.increment(Type.MAX_RATE);

                // confirm we have 1 bucket
                assertEquals(1, counter.buckets.size());

                // confirm we still have 1 bucket
                assertEquals(1, counter.buckets.size());

                // add 1
                counter.increment(Type.MAX_RATE);

                // we should now have a single bucket with no values in it instead of 2 or more buckets
                assertEquals(1, counter.buckets.size());

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testEmptyBucketsFillIn() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // add 1
                counter.increment(Type.MAX_RATE);

                // we should have 1 bucket
                assertEquals(1, counter.buckets.size());

                // wait past 3 bucket time periods (the 1st bucket then 2 empty ones)
                Thread.sleep(counter.getBucketSizeInMilliseconds() * 3);

                // add another
                counter.increment(Type.MAX_RATE);

                // we should have 4 (1 + 2 empty + 1 new one) buckets
                assertEquals(4, counter.buckets.size());

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testIncrementInSingleBucket() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // increment
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.EVENT_DISCARDED);
                counter.increment(Type.EVENT_PROCESSED);

                // we should have 1 bucket
                assertEquals(1, counter.buckets.size());

                // the count should be 4
                assertEquals(4, counter.buckets.getLast().get(Type.MAX_RATE).get());
                assertEquals(2, counter.buckets.getLast().get(Type.MIN_RATE).get());
                assertEquals(1, counter.buckets.getLast().get(Type.EVENT_DISCARDED).get());
                assertEquals(1, counter.buckets.getLast().get(Type.EVENT_PROCESSED).get());

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testEVENT_DISCARDED() {
            testCounterType(Type.EVENT_DISCARDED);
        }

        @Test
        public void testEVENT_PROCESSED() {
            testCounterType(Type.EVENT_PROCESSED);
        }

        @Test
        public void testMAX_RATED() {
            testCounterType(Type.MAX_RATE);
        }

        @Test
        public void testMIN_RATE() {
            testCounterType(Type.MIN_RATE);
        }

        private void testCounterType(Type type) {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // increment
                counter.increment(type);

                // we should have 1 bucket
                assertEquals(1, counter.buckets.size());

                // the count should be 1
                assertEquals(1, counter.buckets.getLast().get(type).get());
                assertEquals(1, counter.getCount(type));

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds() * 3);

                // increment again in latest bucket
                counter.increment(type);

                // we should have 4 buckets
                assertEquals(4, counter.buckets.size());

                // the counts of the last bucket
                assertEquals(1, counter.buckets.getLast().get(type).get());

                // the total counts
                assertEquals(2, counter.getCount(type));

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testIncrementInMultipleBuckets() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // increment
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.EVENT_DISCARDED);
                counter.increment(Type.EVENT_PROCESSED);
                counter.increment(Type.EVENT_PROCESSED);

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds() * 3);

                // increment
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.EVENT_DISCARDED);
                counter.increment(Type.EVENT_DISCARDED);
                counter.increment(Type.EVENT_PROCESSED);

                // we should have 4 buckets
                assertEquals(4, counter.buckets.size());

                // the counts of the last bucket
                assertEquals(2, counter.buckets.getLast().get(Type.MAX_RATE).get());
                assertEquals(3, counter.buckets.getLast().get(Type.MIN_RATE).get());
                assertEquals(2, counter.buckets.getLast().get(Type.EVENT_DISCARDED).get());
                assertEquals(1, counter.buckets.getLast().get(Type.EVENT_PROCESSED).get());

                // the total counts
                assertEquals(6, counter.getCount(Type.MAX_RATE));
                assertEquals(5, counter.getCount(Type.MIN_RATE));
                assertEquals(3, counter.getCount(Type.EVENT_DISCARDED));
                assertEquals(3, counter.getCount(Type.EVENT_PROCESSED));

                // wait until window passes
                Thread.sleep(counter.timeInMilliseconds.get());

                // increment
                counter.increment(Type.EVENT_DISCARDED);

                // the total counts should now include only the last bucket after a reset since the window passed
                assertEquals(0, counter.getCount(Type.MAX_RATE));
                assertEquals(0, counter.getCount(Type.MIN_RATE));
                assertEquals(1, counter.getCount(Type.EVENT_DISCARDED));
                assertEquals(0, counter.getCount(Type.EVENT_PROCESSED));

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testCounterRetrievalRefreshesBuckets() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // increment
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MAX_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.MIN_RATE);
                counter.increment(Type.EVENT_DISCARDED);

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds() * 3);

                // we should have 1 bucket since nothing has triggered the update of buckets in the elapsed time
                assertEquals(1, counter.buckets.size());

                // the total counts
                assertEquals(4, counter.getCount(Type.MAX_RATE));
                assertEquals(2, counter.getCount(Type.MIN_RATE));
                assertEquals(1, counter.getCount(Type.EVENT_DISCARDED));

                // we should have 4 buckets as the counter 'gets' should have triggered the buckets being created to fill in time
                assertEquals(4, counter.buckets.size());

                // wait until window passes
                Thread.sleep(counter.timeInMilliseconds.get());

                // the total counts should all be 0 (and the buckets cleared by the get, not only increment)
                assertEquals(0, counter.getCount(Type.MAX_RATE));
                assertEquals(0, counter.getCount(Type.MIN_RATE));
                assertEquals(0, counter.getCount(Type.EVENT_DISCARDED));

                // increment
                counter.increment(Type.EVENT_DISCARDED);

                // the total counts should now include only the last bucket after a reset since the window passed
                assertEquals(0, counter.getCount(Type.MAX_RATE));
                assertEquals(0, counter.getCount(Type.MIN_RATE));
                assertEquals(1, counter.getCount(Type.EVENT_DISCARDED));

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testSet() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // increment
                counter.set(Type.EVENT_DISCARDED, 10);

                // we should have 1 bucket
                assertEquals(1, counter.buckets.size());

                // the count should be 10
                assertEquals(10, counter.buckets.getLast().get(Type.EVENT_DISCARDED).get());
                assertEquals(10, counter.getCount(Type.EVENT_DISCARDED));

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds() * 3);

                // increment again in latest bucket
                counter.set(Type.EVENT_DISCARDED, 20);

                // we should have 4 buckets
                assertEquals(4, counter.buckets.size());

                // the count
                assertEquals(20, counter.buckets.getLast().get(Type.EVENT_DISCARDED).get());

                // the total counts
                assertEquals(30, counter.getCount(Type.EVENT_DISCARDED));

                // counts per bucket
                int values[] = counter.getValues(Type.EVENT_DISCARDED);
                assertEquals(10, values[0]); // oldest bucket
                assertEquals(0, values[1]);
                assertEquals(0, values[2]);
                assertEquals(20, values[3]); // latest bucket

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testCompareAndSet() {
            try {
                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                // increment
                counter.set(Type.EVENT_DISCARDED, 10);
                assertTrue(counter.compareAndSet(Type.EVENT_DISCARDED, 10, 30));
                assertFalse(counter.compareAndSet(Type.EVENT_DISCARDED, 10, 20));

                // we should have 1 bucket
                assertEquals(1, counter.buckets.size());

                // the count should be 30
                assertEquals(30, counter.buckets.getLast().get(Type.EVENT_DISCARDED).get());
                assertEquals(30, counter.getCount(Type.EVENT_DISCARDED));

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds() * 3);

                // compareAndSet should not work with 10 right now because the time has passed and it is now 0
                assertFalse(counter.compareAndSet(Type.EVENT_DISCARDED, 10, 30));
                assertTrue(counter.compareAndSet(Type.EVENT_DISCARDED, 0, 30));
                int latestValue = counter.getValueOfLatestBucket(Type.EVENT_DISCARDED);
                assertTrue(counter.compareAndSet(Type.EVENT_DISCARDED, latestValue, 50));

                // we should have 4 buckets
                assertEquals(4, counter.buckets.size());

                // the count
                assertEquals(50, counter.buckets.getLast().get(Type.EVENT_DISCARDED).get());
                assertEquals(50, counter.getValueOfLatestBucket(Type.EVENT_DISCARDED));

                // counts per bucket
                int values[] = counter.getValues(Type.EVENT_DISCARDED);
                assertEquals(30, values[0]); // oldest bucket
                assertEquals(0, values[1]);
                assertEquals(0, values[2]);
                assertEquals(50, values[3]); // latest bucket

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testMaxValue() {
            try {
                Type type = Type.EVENT_DISCARDED;

                StatsRollingNumber counter = new StatsRollingNumber(200, 10);

                counter.set(type, 10);

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds());

                counter.set(type, 30);

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds());

                counter.set(type, 40);

                // sleep to get to a new bucket
                Thread.sleep(counter.getBucketSizeInMilliseconds());

                counter.set(type, 15);

                assertEquals(40, counter.getMaxValue(type));

            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception: " + e.getMessage());
            }
        }

        @Test
        public void testEmptySum() {
            Type type = Type.EVENT_DISCARDED;
            StatsRollingNumber counter = new StatsRollingNumber(200, 10);
            assertEquals(0, counter.getSum(type));
        }

        @Test
        public void testEmptyLatestValue() {
            Type type = Type.EVENT_DISCARDED;
            StatsRollingNumber counter = new StatsRollingNumber(200, 10);
            assertEquals(0, counter.getValueOfLatestBucket(type));
        }

        @Test
        public void testRolling() {
            Type type = Type.EVENT_DISCARDED;
            StatsRollingNumber counter = new StatsRollingNumber(20, 2);
            // iterate over 20 buckets on a queue sized for 2
            for (int i = 0; i < 20; i++) {
                // first bucket
                counter.getCurrentBucket();
                try {
                    Thread.sleep(counter.getBucketSizeInMilliseconds());
                } catch (Exception e) {
                    // ignore
                }

                assertEquals(2, counter.getValues(type).length);

                counter.getValueOfLatestBucket(type);

                // System.out.println("Head: " + counter.buckets.state.get().head);
                // System.out.println("Tail: " + counter.buckets.state.get().tail);
            }
        }

    }
}
