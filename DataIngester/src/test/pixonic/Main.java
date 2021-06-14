package test.pixonic;

import test.pixonic.ingester.DataIngester;

public class Main {
	
	public static void main(String[] args) {
		
		
		try {
			
			DataIngester theIngester = new DataIngester(2, 10);
			theIngester.start();
			
			theIngester.add(0, 'a', 1);
			theIngester.add(1, 'b', 1);
			theIngester.add(2, 'a', -3);
			Thread.sleep(3000);
			System.out.println("Case 1:");
			System.out.println(theIngester.sum(0, 3, 'a'));
			System.out.println(theIngester.sum(1, 3, 'b'));
			System.out.println(theIngester.sum(0, 1, 'b'));
			
			theIngester.add(0, 'a', 1);
			theIngester.add(1, 'b', 1);
			theIngester.add(2, 'a', -3);
			Thread.sleep(3000);
			System.out.println("Case 2:");
			System.out.println(theIngester.sum(0, 3, 'a'));
			System.out.println(theIngester.sum(1, 3, 'b'));
			System.out.println(theIngester.sum(0, 1, 'b'));
			
			//Thread.sleep(3000);
			theIngester.stop();
			
			///
			
			theIngester = new DataIngester(10, 10);
			theIngester.start();
			
			Thread p1 = new Thread(new DataProducer(theIngester));
			Thread p2 = new Thread(new DataProducer(theIngester));
			Thread p3 = new Thread(new DataProducer(theIngester));
			Thread p4 = new Thread(new DataProducer(theIngester));
			Thread p5 = new Thread(new DataProducer(theIngester));
			Thread p6 = new Thread(new DataProducer(theIngester));
			p1.start();
			p2.start();
			p3.start();
			p4.start();
			p5.start();
			p6.start();
			
			p1.join();
			p2.join();
			p3.join();	
			p4.join();
			p5.join();
			p6.join();
			Thread.sleep(3000);
			
			System.out.println("Case 3:");
			System.out.println(theIngester.sum(0, 3, 'a'));
			System.out.println(theIngester.sum(1, 3, 'b'));
			System.out.println(theIngester.sum(0, 1, 'b'));
			
			//Thread.sleep(3000);
			theIngester.stop();
			
			
		} catch (Exception e) {
			
		}
		
	}
	
	public static class DataProducer implements Runnable {
		
		private final DataIngester ingester;
		
		public DataProducer(DataIngester ingester) {
			this.ingester = ingester;
		}


		public void run() {
			ingester.add(0, 'a', 1);
			ingester.add(1, 'b', 1);
			ingester.add(2, 'a', -3);
		}
		
	}

}
