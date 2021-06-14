package test.pixonic.ingester;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class KeyValueStorage {
	
	private static Logger logger = Logger.getLogger(KeyValueStorage.class.getName());
	
	public static KeyValueStorage createForKey(char key) {
		
		String fileName = "storage_" + String.valueOf(key);
		
		try (RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw")) {
			
			FileChannel channel = file.getChannel();
			
			MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024);
			
			channel.close();
			
			return new KeyValueStorage(key, buffer);
			
		} catch (ClosedByInterruptException e) { 
			logger.warning(String.format("There is a need to wait stopping main thread a little bit: %s", e.getMessage()));
		} catch (Exception e) {
			logger.warning(String.format("Can't perform createForKey(): %s", e.getMessage()));
		}
		
		return null;
	}

	
	private final char key;
	private final MappedByteBuffer buffer;
	private final ReentrantLock lock;
	
	protected KeyValueStorage(char key, MappedByteBuffer buffer) {
		this.key = key;
		this.buffer = buffer;
		lock = new ReentrantLock();
	}
	
	public void putValue(long timestamp, int value) throws InterruptedException {
		
		lock.lockInterruptibly();
		try {
			
			buffer.putLong(timestamp);
			buffer.putInt(value);
		
		} finally {
			lock.unlock();
		}
	}
	
	public long sumForInterval(long startTimestampInclusive, long endTimestampExclusive) {
		
		long result = 0;
		
		int endPosition = buffer.position();
		
		for (int idx = 0; idx < endPosition;) {
			
			long timestamp = buffer.getLong(idx);
			int value = buffer.getInt(idx + 8);
			if (timestamp >= startTimestampInclusive && timestamp < endTimestampExclusive) {
				result += value;
			}
			
			idx += 12;
		}
		
		return result;
	}
	
}
