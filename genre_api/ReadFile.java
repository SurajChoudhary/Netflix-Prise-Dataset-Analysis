import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLEncoder;

public class ReadFile {
	
	
	// read file and separate with comma
	public static String[] readFile() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("movie_titles.txt"));
		int index = -1;
		String[] movieNamesArray = new String[17770];
		
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line;
	        
	        
	        while ( (line = br.readLine()) != null) {
	        	
	            sb.append(line);
	            sb.append(System.lineSeparator());	            
	            //System.out.println(line);

	            String[] recordArray = line.split(",");
		        String movieName = recordArray[2];
		        if (movieName.contains(":")) {
		        	movieNamesArray[++index] = movieName.split(":")[0];
		        } else {
		        	movieNamesArray[++index] = movieName;
		        }
		        
	            
	        }

	        
	    } finally {
	        br.close();
	    }
	    return movieNamesArray;
	}
	
	
	public static String[] constructURL(String[] movieNamesArray) {
		String[] encodeURLArray = new String[17770];
		int index = -1;
		for (String name : movieNamesArray) {
			@SuppressWarnings("deprecation")
			String encodeURL = URLEncoder.encode(name);
			encodeURLArray[++index] = encodeURL;
			//System.out.println(encodeURL);
		}
		return encodeURLArray;
	}
	
	@SuppressWarnings("deprecation")
	public static String reconstructURL(String url) {
		String[] urlArray = url.split(" ");
		
		if (urlArray.length > 0) {
			String name = urlArray[0];
			//name += urlArray[1];

			String encodeURL = URLEncoder.encode(name);
			return encodeURL;
		}
		
		return URLEncoder.encode(url);
		
	}

	// test
	public static void main(String[] args) throws IOException {
		String[] movieNamesArray = readFile();
		System.out.println(movieNamesArray[0]);
		constructURL(movieNamesArray);
//		for (String s: movieNamesArray) {
//			System.out.println(s);
//		}

	}

}
