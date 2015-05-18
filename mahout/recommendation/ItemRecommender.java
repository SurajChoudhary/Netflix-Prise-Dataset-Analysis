import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

public class ItemRecommender {
	
	public static void recommend(String userId, int noOfRecommendations) {

		try {
			// Data model created to accept the input file
			FileDataModel dataModel = new FileDataModel(new File(
					// [movieID, userID, rating]
					"src/main/resources/dataset/item_out.txt"));

			// Specifies the Similarity algorithm
			ItemSimilarity itemSimilarity = new PearsonCorrelationSimilarity(
					dataModel);

			// Initializing the recommender
			ItemBasedRecommender recommender = new GenericItemBasedRecommender(
					dataModel, itemSimilarity);

			// calling the recommend method to generate recommendations
			List<RecommendedItem> recommendations = recommender.recommend(
					Integer.parseInt(userId), noOfRecommendations);

			// looping through recommendations
			for (RecommendedItem recommendedItem : recommendations) {
				System.out.println(recommendedItem);
			}
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args) {
		// specifying the user id to which the recommendations have to be 
		// generated for as first argument
		// specifying the number of recommendations to be generated as second argument
		recommend("25", 10);
	}
}
