import java.io.File;  // Import the File class
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import java.util.Random; 
import java.util.ArrayList;

public class DataGenerator {
    
	public void customerDataGen() {
		// Generating 50,000 records of customer data 
		// ID: unique sequential number (integer) from 1 to 50,000 (that is the file will have 50,000 line) 
		// Name: random sequence of characters of length between 10 and 20 (do not include commas) 
		// Age: random number (integer) between 10 to 70
		// Gender: string that is either “male” or “female”
		// CountryCode: random number (integer) between 1 and 10
		// Salary: random number (float) between 100 and 10000
		
		StringBuilder data = new StringBuilder(""); 
		String nameCharSet = "abcdefghijklmnopqrstuvwxyz";
		ArrayList <String> gender = new ArrayList<String>();
		gender.add("male");
		gender.add("female");
		Random rand = new Random();
		for (int i = 1; i <= 50000; i++) {
			data.append(Integer.toString(i) + ",");  // add ID
			String name = "";
			int namelen = rand.nextInt(10)+10;
			for (int j = 0; j<namelen; j++) {
				int idx = rand.nextInt(26);
				name += nameCharSet.charAt(idx);
			}
			data.append(name+",");      // add name
			data.append(Integer.toString(rand.nextInt(61)+(int)10)+ ",");
			int genderIdx = rand.nextInt(2);
			data.append(gender.get(genderIdx) + ",");  //add gender
			data.append(Integer.toString(rand.nextInt(10) + (int)1) + ",");  // add CountryCode
			data.append(Float.toString((float)100 + rand.nextFloat() * (float)9900));  // add Salary
			data.append("\n");  //end this record
			//if (i==1) {System.out.println(data);}
		} 
		
		try {
		      File customer = new File("customer.txt");
		      if (customer.createNewFile()) {
		        System.out.println("File created: " + customer.getName());
		      } else {
		        System.out.println("File already exists.");
		      }
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
		try {
			FileWriter fw = new FileWriter("customer.txt");
			fw.write(data.toString());
			fw.close();
		    } catch (IOException e) {
		    	System.out.println("An error occurred.");
		        e.printStackTrace();
	        }
	}
	
	private String TransDescGen(String charset) {
        // Helper for transactionDataGen method
		// Generating a transaction discriminatory code for a transaction
		// String charset = "abcdefghijklmnopqrstuvwxyz0123456789";
		Random rand1 = new Random();
		int len = rand1.nextInt(31) + 20;
		String s = "";
		for (int j = 0; j<len; j++) {
			int idx = rand1.nextInt(36);
			s += charset.charAt(idx);
		}
        return s;
	}
	
	private String TransDescFixLenGen(String charset,int len) {
        // Helper for transactionDataGen method
		// Generating a transaction discriminatory code for a transaction, with a fixed length
		// String charset = "abcdefghijklmnopqrstuvwxyz0123456789";
		Random rand1 = new Random();
		//int len = rand1.nextInt(31) + 20;
		String s = "";
		for (int j = 0; j<len; j++) {
			int idx = rand1.nextInt(36);
			s += charset.charAt(idx);
		}
        return s;
	}
	
	public void transactionDataGen(int n) {
		//Generating  n (5,000,000) records of transaction data 
		//TransID: unique sequential number (integer) from 1 to 5,000,000 (the file has 5M transactions) 
		//CustID: References one of the customer IDs, i.e., from 1 to 50,000 (on Avg. a customer has 100 trans.) 
		//TransTotal: random number (float) between 10 and 1000
		//TransNumItems: random number (integer) between 1 and 10
		//TransDesc: random text of characters of length between 20 and 50 (do not include commas)
		
		StringBuilder data = new StringBuilder(); 
		Random rand = new Random();
		String charset = "abcdefghijklmnopqrstuvwxyz0123456789";
		for (int i = 1; i <= n; i++) {
			data.append((Integer.toString(i) + ","));  // add TransID
			data.append((Integer.toString(rand.nextInt(50000)+(int)1) + ","));  //add CustID
			data.append((Float.toString((float)10 + rand.nextFloat() * (float)990) + ","));  // add TransTotal
			data.append((Integer.toString((int)1 + rand.nextInt(10)) + ","));  // add TransNumItems
			//data += this.TransDescFixLenGen(charset,20); // add TransDesc with fixed length of 20
			data.append(this.TransDescGen(charset)); // add TransDesc
			data.append("\n");  //end this record
			if (i%500000==0) {System.out.println(i/500000 + "/10 complete");}
		} 
		
		try {
		      File transaction = new File("transaction.txt");
		      if (transaction.createNewFile()) {
		        System.out.println("File created: " + transaction.getName());
		      } else {
		        System.out.println("File already exists.");
		      }
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
		try {
			FileWriter fw = new FileWriter("transaction.txt");
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
		dg.customerDataGen();
		dg.transactionDataGen(5000000);
		System.out.println("Data generated, pwd:" + System.getProperty("user.dir"));
	}

}
