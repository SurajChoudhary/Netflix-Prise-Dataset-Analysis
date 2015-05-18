import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;


public class Genre {
	private static final String USER_AGENT = "Mozilla/5.0";
	
	//
	public static String sentGetRequest(String encodeURL) throws Exception {
		String url = "http://www.omdbapi.com/?t=" + encodeURL + "&y=&plot=short&r=json";
		 
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
 
		//add request header
		con.setRequestProperty("User-Agent", USER_AGENT);
 
		int responseCode = con.getResponseCode();
//		System.out.println("\nSending 'GET' request to URL : " + url);
//		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		String result = response.toString();

		in.close();

		return result;
	}
	
	public static String getGenre(String result) {
		JSONObject jsonObj;
		String genre = null;
		try {
			jsonObj = new JSONObject(result);
			//System.out.println ("jsonObj=" + jsonObj);
			
			// check if the response is true
			String response = jsonObj.getString("Response");
			if (response.equalsIgnoreCase("true")) {
				genre = jsonObj.getString("Genre");
				//System.out.println(genre);
			} else {
				genre = null;
			}

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return genre;
	}

}
