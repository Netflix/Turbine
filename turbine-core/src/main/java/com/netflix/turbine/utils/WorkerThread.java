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
package com.netflix.turbine.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that manages the life cycle of a worker thread in a thread safe and idempotent manner. 
 * Implementers must provide functionality for 
 * <ul>
 * <li>Initing resources
 * <li>Doing work periodically
 * <li>Cleanup on shutdown
 * </ul>
 *
 * <p>Users of the object do have control on how often the work happens and if there is an sleep between intervals at all. 
 * <p>They also have explicity control to start and stop the work in a separate thread. 
 */
public class WorkerThread {
    private static final Logger logger = LoggerFactory.getLogger(WorkerThread.class);
    
    private final ExecutorService threadPool;
    private final boolean sleep; 
    private final int sleepMillis;
    private final Worker worker; 
    private volatile boolean stopWorker = false; 
    private volatile Future<Void> future;
    
    /**
     * Interface for the functionality of a worker object
     */
    public interface Worker {
        
        /**
         * Init all required resources
         * @throws Exception
         */
        public void init() throws Exception; 
        
        /**
         * Do work periodically
         * @throws Exception
         */
        public void doWork() throws Exception; 
        
        /**
         * Cleanup resources that were created
         * @throws Exception
         */
        public void cleanup() throws Exception; 
    }
    
    /**
     * @param worker
     */
    public WorkerThread(Worker worker) {
        this(worker, 15000, true);  // default sleep is 15 seconds
    }

    /**
     * @param worker
     * @param sleep
     */
    public WorkerThread(Worker worker, boolean sleep) {
        this(worker, 15000, sleep);  
    }

    /**
     * @param worker
     * @param sleepMillis
     */
    public WorkerThread(Worker worker, int sleepMillis) {
        this(worker, sleepMillis, true);  
    }

    /**
     * @param worker
     * @param sleepMillis
     * @param sleep
     */
    public WorkerThread(Worker worker, int sleepMillis, boolean sleep) {
        this.worker = worker;
        this.sleepMillis = sleepMillis;
        this.sleep = sleep;
        threadPool = Executors.newSingleThreadExecutor();
    }
    
    /**
     * Start the work, must happen once and only once. 
     * @throws Exception
     */
    public void start() throws Exception {

        if(isRunningTask()) {
            return;
        }
        
        try {
            worker.init(); 
        } catch(Throwable t) {
            stopWorker = true;
            worker.cleanup();
            threadPool.shutdownNow();
            throw new Exception(t);
        }

        future = threadPool.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {

                try {
                    while(!stopWorker) {
                        worker.doWork();
                        if(sleep && !stopWorker) {
                            Thread.sleep(sleepMillis);
                        }
                    }
                
                    // shut down
                    return null;
                    
                } catch (InterruptedException ex) {
                    // shut down
                    logger.info("Worker got interrupted, shutting down");
                    return null;
                } catch (Throwable t) {
                    // shut down
                    logger.info("Worker caught throwable, shutting down", t);
                    return null;
                } finally {
                    stopWorker = true;
                    worker.cleanup();
                    threadPool.shutdownNow();
                }
            }
        });
    }
    
    /**
     * Request worker to stop
     */
    public void stop() {
        stopWorker = true;
        if(future != null && !future.isDone() && !future.isCancelled()) {
            future.cancel(true);
        }
        threadPool.shutdownNow();
    }

    /**
     * Stop and block till worker is finished
     */
    public void stopAndBlock() {
        stop();
        while(!threadPool.isTerminated()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                return; // return if interrupted
            }
        }
    }
    
    /**
     * Helper to identify if worker is still running
     * @return true/false
     */
    public boolean isRunning() {
        return !threadPool.isTerminated();
    }
    
    private boolean isRunningTask() {
        return (future != null && !future.isDone() && !future.isCancelled());
    }
    
    /**
     * Helper to identify is work has been requested to stop
     * @return true/false
     */
    public boolean isStopRequested() {
        return stopWorker;
    }
    
    public static class UnitTest {
        
        @Test
        public void testNormalFlow() throws Exception {
            
            Worker worker = mock(Worker.class);
            
            WorkerThread thread = new WorkerThread(worker, 20, true);  // sleep for just 20 millis
            thread.start();
            
            verify(worker, atLeastOnce()).init();
            verify(worker, atMost(1)).init();
            
            Thread.sleep(200);
            
            verify(worker, atLeastOnce()).doWork();

            assertTrue(thread.isRunning());
            assertTrue(thread.isRunningTask());
            
            thread.stop();

            Thread.sleep(100);
            
            verify(worker, atLeastOnce()).cleanup();
            verify(worker, atMost(1)).cleanup();
            
            assertFalse(thread.isRunning());
            assertFalse(thread.isRunningTask());
        }
        
        @Test
        public void testBlowUpOnInit() throws Exception {
            
            Worker worker = mock(Worker.class);
            
            doAnswer(new Answer<Void>() {

                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    throw new Exception("Kaboom!");
                }
            }).when(worker).init();
            
            WorkerThread thread = new WorkerThread(worker, 20, true);  // sleep for just 20 millis
            
            try {
                thread.start();
            } catch (Exception e) {
            }
            
            verify(worker, atLeastOnce()).init();
            verify(worker, atMost(1)).init();  // looking for exactly once actually.
            
            Thread.sleep(200);
            
            verify(worker, never()).doWork();

            assertFalse(thread.isRunning());
            assertFalse(thread.isRunningTask());
            
            verify(worker, atLeastOnce()).cleanup();
            verify(worker, atMost(1)).cleanup();
        }

        @Test
        public void testBlowUpOnDoWork() throws Exception {
            
            Worker worker = mock(Worker.class);
            
            doAnswer(new Answer<Void>() {
                int count = 0;
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    count++; 
                    if(count <= 10) {
                        return null;
                    } else {
                        throw new Exception("Kaboom!");
                    }
                }
            }).when(worker).doWork();
            
            WorkerThread thread = new WorkerThread(worker, 20, true);  // sleep for just 20 millis
            
            thread.start();
            
            verify(worker, atLeastOnce()).init();
            verify(worker, atMost(1)).init();  // looking for exactly once actually.
            
            Thread.sleep(500);
            
            verify(worker, atMost(11)).doWork();

            assertFalse(thread.isRunning());
            assertFalse(thread.isRunningTask());
            
            verify(worker, atLeastOnce()).cleanup();
            verify(worker, atMost(1)).cleanup();
        }
    
        @Test
        public void testStartIsIdempotent() throws Exception {
            
            Worker worker = mock(Worker.class);
            
            WorkerThread thread = new WorkerThread(worker, 20, true);  // sleep for just 20 millis
            thread.start();
            
            Thread.sleep(200);
            
            verify(worker, atLeastOnce()).doWork();

            assertTrue(thread.isRunning());
            assertTrue(thread.isRunningTask());

            for(int i=0; i<1000; i++) {
                thread.start();
            }
            
            verify(worker, atLeastOnce()).init();
            verify(worker, atMost(1)).init();
            
            thread.stop();

            Thread.sleep(100);
            
            verify(worker, atLeastOnce()).cleanup();
            verify(worker, atMost(1)).cleanup();
            
            assertFalse(thread.isRunning());
            assertFalse(thread.isRunningTask());
        }
    }
}
