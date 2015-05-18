import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


public class WriteFile {
	
	public static void writeToFile(int index, String genre) {

		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("movie_genres.txt", true)));
		    out.println(index + "," + genre);
		    out.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
		
	}
	
	

}
