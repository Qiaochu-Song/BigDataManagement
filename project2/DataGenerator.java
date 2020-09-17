//package proj02;

import java.io.File;  // Import the File class
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import java.util.Random; 

public class DataGenerator {
    // private int numPoints;
    // private int numRects;
    // assume the space extends from 1...10,000 in both the x and y axis.

    private void pointGenerator(int numPoints) {
    	StringBuilder data = new StringBuilder(""); 

		Random rand = new Random();
		for (int i = 0; i < numPoints; i++) {
			int x = rand.nextInt(10000) + 1;
			int y = rand.nextInt(10000) + 1;
			data.append(x + "," + y + "\n");  // use '\n' to end this record
		} 
		
		try {
		      File points = new File("points.txt");
		      if (points.createNewFile()) {
		        System.out.println("File created: " + points.getName());
		      } else {
		        System.out.println("File already exists.");
		      }
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
		try {
			FileWriter fw = new FileWriter("points.txt");
			fw.write(data.toString());
			fw.close();
		    } catch (IOException e) {
		    	System.out.println("An error occurred.");
		        e.printStackTrace();
	        }
	}
    
    private void rectGenerator(int numRects) {
    	// The data format for rectangles is (bottomLeft_x, bottomLeft_y, height, width)
		// the height random variable can be uniform between [1,10]
		// the width random is also uniform between [1,20].
    	StringBuilder data = new StringBuilder("");
    	Random rand = new Random();
    	for (int i = 0; i < numRects; i++) {
			int height = rand.nextInt(10) + 1;
			int width = rand.nextInt(20) + 1;
			int BLx = rand.nextInt(10000-width) + 1;
			int BLy = rand.nextInt(10000-height) + 1;
			data.append(BLx + "," + BLy + "," + height + "," + width + "\n");  // use '\n' to end this record
		} 
		
		try {
		      File rects = new File("rectangles.txt");
		      if (rects.createNewFile()) {
		        System.out.println("File created: " + rects.getName());
		      } else {
		        System.out.println("File already exists.");
		      }
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
		try {
			FileWriter fw = new FileWriter("rectangles.txt");
			fw.write(data.toString());
			fw.close();
		    } catch (IOException e) {
		    	System.out.println("An error occurred.");
		        e.printStackTrace();
	        }
    	
    }
    public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataGenerator dg = new DataGenerator();
        dg.pointGenerator(12000000);
        dg.rectGenerator(8000000);
        System.out.println("Data generated, pwd:" + System.getProperty("user.dir"));
	}

}
