package test.pixonic.ingester;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class DataIngester {
	
	private static Logger logger = Logger.getLogger(DataIngester.class.getName());
	
	private final ConcurrentHashMap<Character, Object> storage;
	private final ArrayList<ArrayBlockingQueue<Object>> reel;
	private final Thread[] readers;
	private final AtomicInteger reelPosition;
	private final int reelCapacity;
	
	public DataIngester(int streamNumber, int streamCapacity) {
		
		reelCapacity = streamNumber;
		storage = new ConcurrentHashMap<Character, Object>();
		reel = new ArrayList<>(reelCapacity);
		readers = new Thread[reelCapacity];
		reelPosition = new AtomicInteger(0);
		
		logger.info(String.format("DataIngester was created for %d streams and %d stream capacity", reelCapacity, streamCapacity));
	}
	
	public void start() {
		
		for (int idx = 0; idx < reelCapacity; idx ++) {
			reel.add(idx, new ArrayBlockingQueue<Object>(reelCapacity));
			readers[idx] = startReader(reel.get(idx));
		}
		
		logger.info("DataIngester started");
	}
	
	public void stop() {
		for (int idx = 0; idx < reelCapacity; idx ++) {
			try {
				readers[idx].interrupt();
			} catch (Exception e) {
				logger.warning(String.format("Can't stop thread: %s", e.getMessage()));
			}
		}
		
		logger.info("DataIngester stopped");
	}
	
	public void add(long timestamp, char key, int value) {
		
		DataElement element = new DataElement(timestamp, key, value);
		
		int currentPosition = reelPosition.getAndIncrement();
		int currentIndex = currentPosition % reelCapacity;
		try {
			reel.get(currentIndex).put(element);
		} catch (InterruptedException e) {
			logger.warning(String.format("Can't add value: %d", e.getMessage()));
		}
		
	}
	
	public long sum(long startTimestampInclusive, long endTimestampExclusive, char key) {
	
		KeyValueStorage valueStorage = (KeyValueStorage)storage.get(key);
		if (valueStorage != null) {
			return valueStorage.sumForInterval(startTimestampInclusive, endTimestampExclusive);
		}
		
		return 0;
	}
	
	private Thread startReader(ArrayBlockingQueue<Object> queue) {
		
		Thread reader = new Thread(new DataReader(queue));
		reader.start();
		
		return reader;
	}
	
	public class DataReader implements Runnable {
		
		private final ArrayBlockingQueue<Object> queue;
			
		public DataReader(ArrayBlockingQueue<Object> queue) {
			this.queue = queue;
		}

		public void run() {
			
			while (!Thread.currentThread().isInterrupted()) {
				
				DataElement element =  null;
										
				try {
						
					element = (DataElement)queue.take();
					if (element != null) {
						
						KeyValueStorage valueStorage = (KeyValueStorage)storage.get(element.key);
						if (valueStorage == null) {
							storage.putIfAbsent(element.key, KeyValueStorage.createForKey(element.key));
							valueStorage = (KeyValueStorage)storage.get(element.key);
						}
						
						valueStorage.putValue(
							Long.valueOf(element.timestamp), 
							Integer.valueOf(element.value)
						);
					}
					
				} catch (InterruptedException e) {
					return;
				} catch (NullPointerException e) {
					continue;
				}
			}
			
		}
		
	}

}
