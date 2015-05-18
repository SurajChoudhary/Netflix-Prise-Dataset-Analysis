package bigdata.hbase.quan.store;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class StoreMoviesInfo {

	static Configuration config;
	//static HTable table;
	static Put movies;

	// Create training_Set tables
	public static void initDatabase() throws IOException {
		config = HBaseConfiguration.create();
		// create an Admin object using the configuration
		HBaseAdmin admin = new HBaseAdmin(config);
		// specify the zookeeper IP and proceed
		config.set("hbase.zookeeper.quorum", "192.168.0.101:2181");
		// create the table...
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf("movies"));
		// create two column families
		tableDescriptor.addFamily(new HColumnDescriptor("yf"));
		tableDescriptor.addFamily(new HColumnDescriptor("nf"));
		tableDescriptor.addFamily(new HColumnDescriptor("gf"));
		admin.createTable(tableDescriptor);

		admin.close();
	}

	public static void createColumnFamily(String movieID, String releaseYear,
			String movieName, String[] genresArray) throws IOException {
		HTable table = new HTable(config, "movies");
		// create the row key
		// System.out.println("create the row key" + movieID);
		Put movies = new Put(Bytes.toBytes(movieID));

		// create column families
		movies.add(Bytes.toBytes("yf"), Bytes.toBytes("year"),
				Bytes.toBytes(releaseYear));
		movies.add(Bytes.toBytes("nf"), Bytes.toBytes("name"),
				Bytes.toBytes(movieName));
		for (int i = 1; i < genresArray.length; i++) {
			movies.add(Bytes.toBytes("gf"), Bytes.toBytes("genre" + i),
					Bytes.toBytes(genresArray[i].trim()));
		}

		table.put(movies);
		table.flushCommits();
		table.close();

	}

	// pre-process text file
	public static void readMoviesFile() throws FileNotFoundException {
		//
		String path = "src/main/java/bigdata/hbase/quan/data/movie_titles.txt";
		BufferedReader br = new BufferedReader(new FileReader(path));
		//
		String path1 = "src/main/java/bigdata/hbase/quan/data/movie_genres.txt";
		BufferedReader br1 = new BufferedReader(new FileReader(path1));

		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			StringBuilder sb1 = new StringBuilder();
			String line1 = br1.readLine();

			while (((line = br.readLine()) != null)
					&& ((line1 = br1.readLine()) != null)) {

				sb.append(line);
				sb.append(System.lineSeparator());

				sb1.append(line1);
				sb1.append(System.lineSeparator());

				// movie_titles.txt
				String[] dataArray = line.split(",");
				String movieID = dataArray[0];
				String releaseYear = dataArray[1];
				String movieName = dataArray[2];
				// movie_genres.txt
				String[] genresArray = line1.split(",");

				createColumnFamily(movieID, releaseYear, movieName, genresArray);
				System.out.print(movieID + ",");
				System.out.print(releaseYear + ",");
				System.out.print(movieName + "     |");

				for (int i = 1; i < genresArray.length; i++) {
					System.out.print(genresArray[i].trim() + "|");
				}

				System.out.println();

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

//	public static void closeTable() {
//
//		try {
//			table.flushCommits();
//			table.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

	public static void main(String[] args) throws IOException {
		initDatabase();
		readMoviesFile();
	}

}
