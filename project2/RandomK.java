//package proj02;

import java.io.File;  // Import the File class
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import java.util.Random; 

public class RandomK {
    public void generateRandomK(int k) {
    	StringBuilder data = new StringBuilder(""); 

		Random rand = new Random();
		for (int i = 0; i < k; i++) {
			int x = rand.nextInt(10000) + 1;
			int y = rand.nextInt(10000) + 1;
			data.append((i+1)+"\t"+ x + "," + y + ",0\n");  // use '\n' to end this record
		} 
		
		try {
		      File randomK = new File("randomK.txt");
		      if (randomK.createNewFile()) {
		        System.out.println("File created: " + randomK.getName());
		      } else {
		        System.out.println("File already exists.");
		      }
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
		try {
			FileWriter fw = new FileWriter("randomK.txt");
			fw.write(data.toString());
			fw.close();
		    } catch (IOException e) {
		    	System.out.println("An error occurred.");
		        e.printStackTrace();
	        }
    }
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.out.println("input format: int k");
		}
		else {
		RandomK rk = new RandomK();
		rk.generateRandomK(Integer.parseInt(args[0]));
		//rk.generateRandomK(9); // for test
		System.out.println("Initial random K centers generated, pwd:" + System.getProperty("user.dir"));
		}
	}

}
