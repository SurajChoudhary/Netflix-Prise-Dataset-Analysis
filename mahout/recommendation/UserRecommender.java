import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class UserRecommender {

	public static void recommend(String userID, int noOfRecommendations)
			throws IOException, TasteException {

		// Data model created to accept the input file
		DataModel model = new FileDataModel(new File(
				"src/main/resources/dataset/item_out.txt"));
		/* Specifies the Similarity algorithm */
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

		/*
		 * NearestNUserNeighborhood is preferred in situations where we need to
		 * have control on the exact no of neighbors
		 */
		UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.2,
				similarity, model);

		/* Initializing the recommender */
		UserBasedRecommender recommender = new GenericUserBasedRecommender(
				model, neighborhood, similarity);

		// calling the recommend method to generate recommendations
		List<RecommendedItem> recommendations = recommender.recommend(
				Integer.parseInt(userID), noOfRecommendations);

		for (RecommendedItem recommendation : recommendations) {
			System.out.println(recommendation);
		}
	}

	public static void main(String[] args) throws IOException, TasteException {
		recommend("25", 10);
	}

}
