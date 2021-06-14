package test.pixonic.ingester;

public class DataElement {
	
	public final long timestamp;
	public final char key; 
	public final int value;
	
	public DataElement(long timestamp, char key, int value) {
		this.timestamp = timestamp;
		this.key = key;
		this.value = value;
	}

}
