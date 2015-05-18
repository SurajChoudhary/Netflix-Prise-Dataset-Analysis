
public class Test {
	
	public static void main(String[] args) {
		
		int counter = 0;
		int index = 1;
		
		try {
			
			String[] movieNamesArray = ReadFile.readFile();
			//System.out.println(movieNamesArray[0]);
			String[] encodeURLArray = ReadFile.constructURL(movieNamesArray);
			
			
			//test
			for (int i = 0; i < 17770; i++) {
				System.out.println("---------------" + encodeURLArray[i]);
				String result = Genre.sentGetRequest(encodeURLArray[i]);
				String genre = Genre.getGenre(result);
				
				if (genre == null) {
					String url = ReadFile.reconstructURL(movieNamesArray[i]);
					result = Genre.sentGetRequest(url);
					genre = Genre.getGenre(result);
				}
				
				if (genre == null) {
					counter++;
				}
				WriteFile.writeToFile(index, genre);
				System.out.print((index++) + " ");
				System.out.println("Genre = " + genre);
				
			}

			System.out.println("COUNTER = " + counter);
			
//			for (String encodeURL: encodeURLArray) {
//				String result = Genre.sentGetRequest(encodeURL);
//				String genre = Genre.getGenre(result);
//				System.out.println("Genre = " + genre);
//			}
			
		
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
