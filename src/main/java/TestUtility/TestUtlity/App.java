package TestUtility.TestUtlity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

/**
 * Hello world!
 *
 */
public class App {

	private static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException {

		// processCSV();

		new uploadCSV().processCSV();

		// testCSV();

		System.out.println("reading file for configmap and kubernetes world ");
		String filePath = "/etc/config/utilityEnv"; // File path in the mounted

		String secretPath = "/etc/secret/utilitysecretcredential";
		// volume
		// String mainpath =
		// "C:/Users/VIRENDERSINGH/Virender/Software/Projects/Spring/TestUtlity/src/main/resources";
		// String filePath = "app.properties";
		try {
			// File file = new File(mainpath + "/" + filePath);
			File file = new File(filePath);
			logger.info("file {}", file.getName());
			logger.info("file is file {}", file.isFile());
			logger.info("file path {}", file.getAbsolutePath());
			FileReader fileReader = new FileReader(file);
			logger.info("fileReader {}", fileReader);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			logger.info("bufferedReader {}", bufferedReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				System.out.println("printing===" + line); // Print each line of the file (ConfigMap value)
			}
			bufferedReader.close();
		} catch (IOException e) {
			System.out.println("Exception Occured==== " + e.getMessage());
		}

		logger.info("******************************************************");

		try {
			// File file = new File(mainpath + "/" + filePath);
			File file1 = new File(secretPath);
			logger.info("file {}", file1.getName());
			logger.info("file is file {}", file1.isFile());
			logger.info("file path {}", file1.getAbsolutePath());
			FileReader fileReader1 = new FileReader(file1);
			logger.info("fileReader1 {}", fileReader1);
			BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
			logger.info("bufferedReader1 {}", bufferedReader1);
			String line;
			while ((line = bufferedReader1.readLine()) != null) {
				System.out.println("printing===" + line); // Print each line of the file (ConfigMap value)
			}
			bufferedReader1.close();
		} catch (IOException e) {
			System.out.println("Exception Occured==== " + e.getMessage());
		}

		// try (InputStream input =
		// App.class.getClassLoader().getResourceAsStream(filePath)) {
		// try (InputStream input = new FileInputStream(mainpath + "/" + filePath)) {

		logger.info("******************************************************");
		try (InputStream input = new FileInputStream(filePath)) {
			Properties prop = new Properties();

			if (input == null) {
				System.out.println("Sorry, unable to find app.properties");
				logger.info("Sorry, unable to find app.properties {}");
				// return;
			}

			String serverUrl = null;
			prop.load(input);

			String datasource = prop.getProperty("datasource");
			String login = prop.getProperty("login");
			String password = prop.getProperty("password");

			logger.info("datasource from {}", datasource);

			logger.info("Example log from {}", App.class.getSimpleName());
			logger.info("******************************************************");

			try (InputStream input1 = new FileInputStream(secretPath)) {
				Properties prop1 = new Properties();

				if (input1 == null) {
					System.out.println("Sorry, unable to find app.properties");
					logger.info("Sorry, unable to find app.properties {}");
					// return;
				}
				prop1.load(input1);
				logger.info("props1 login  {}", prop1.get("login"));
				logger.info("props1 password  {}", prop1.get("password"));
				String login1 = prop1.getProperty("login");
				String password1 = prop1.getProperty("password");

				logger.info("login from secret {}", login1);
				logger.info("password from secret {}", password1);

				logger.info("Example log from secret{}", App.class.getSimpleName());
			}
		}
	}

	public static void processCSV() {
		// Data to be written in the CSV file
		String[] header = { "Name", "Age", "City" };
		String[] row1 = { "John Doe", "30", "New York" };
		String[] row2 = { "Jane Smith", "25", "Los Angeles" };
		String[] row3 = { "Mark Johnson", "35", "Chicago" };

		String csvFile = "example.csv"; // File name for the CSV file
		String bucketName = "odm-cloud-bucket-uat-mig-dct"; // GCS bucket name

		// Write data to the CSV file
		try (FileWriter writer = new FileWriter(csvFile)) {
			// Write the header to the CSV file
			writer.write(String.join(",", header));
			writer.write("\n");

			// Write the rows to the CSV file
			writer.write(String.join(",", row1));
			writer.write("\n");
			writer.write(String.join(",", row2));
			writer.write("\n");
			writer.write(String.join(",", row3));
			writer.write("\n");

			System.out.println("CSV file created successfully.");

			// Upload CSV file to GCS bucket
			Storage storage = getStorageClient();
			BlobId blobId = BlobId.of(bucketName, csvFile);
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
			try (OutputStream outputStream = (OutputStream) storage.writer(blobInfo);
					BufferedWriter bufferedWriter = new BufferedWriter(
							Channels.newWriter((WritableByteChannel) outputStream, "UTF-8"))) {
				FileReader fileReader = new FileReader(csvFile);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					bufferedWriter.write(line);
					bufferedWriter.newLine();
				}
				bufferedReader.close();
				System.out.println("CSV file uploaded to GCS bucket successfully.");
			}
		} catch (IOException | StorageException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	// Create and return a GCS Storage client
	private static Storage getStorageClient() throws IOException {
		FileInputStream credentialsStream = new FileInputStream("path/to/credentials.json"); // Replace with path to
																								// your GCS credentials
																								// file
		StorageOptions options = StorageOptions.newBuilder()
				.setCredentials(ServiceAccountCredentials.fromStream(credentialsStream)).build();
		return options.getService();
	}

	public static void testCSV() {
		// Create a StringBuilder to hold the CSV data
		StringBuilder csvData = new StringBuilder();

		// Add the main header to the StringBuilder
		csvData.append("Main Header");

		// Add the subheaders to the StringBuilder below the main header
		csvData.append("\nSubheader 1  \t");
		csvData.append(" Subheader 2");

		// Add the double values to the StringBuilder below the subheaders
		csvData.append(String.format("\nTest1: %.2f" + (char) 10 + "Test2: %.2f", 1.23, 4.56));
		csvData.append(String.format("\nTest1: %.2f" + (char) 10 + "Test2: %.2f", 7.89, 10.11));
		// Write the CSV data to a file
		String filePath = "example1.csv"; // Set the file path
		try (FileWriter writer = new FileWriter(filePath)) {
			writer.write(csvData.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(filePath + "  file created successfully.");
	}

}
