package TestUtility.TestUtlity;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class uploadCSV {

	private static final Logger LOGGER = LoggerFactory.getLogger(uploadCSV.class);

	public void processCSV() {
		// Data to be written in the CSV filePurgeRuleCSVUpload
		String[] header = { "Name", "Age", "City" };
		String[] row1 = { "John Doe", "30", "New York" };
		String[] row2 = { "Jane Smith", "25", "Los Angeles" };
		String[] row3 = { "Mark Johnson", "35", "Chicago" };

		// String csvFile =
		// "C:\\Users\\VIRENDERSINGH\\Virender\\Software\\Projects\\Spring\\TestUtlity\\purgeCSV\\purge-rule.csv";
		// // File
		// name
		// for
		// the
		// CSV
		String csvFile = "purge-rule.csv"; // File
		// name
		// for
		// the
		// CSV
		// file // file

		String bucketName = "odm-cloud-bucket-uat-mig-dct"; // GCS bucket name

		// The ID of your GCS object
		String objectName = "odm/dev/purgeCSV/" + csvFile; // File
		// name
		// for
		// the
		// CSV
		// file // file

		;

		// The path to your file to upload
		String filePath = csvFile;

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

			LOGGER.info("CSV file created successfully {}", csvFile);

			// Upload CSV file to GCS bucket

			// Storage storage =
			// StorageOptions.newBuilder().setProjectId(projectId).build().getService();
			// BlobId blobId = BlobId.of(bucketName, objectName);
			// BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

			Storage storage = getStorageClient();
			LOGGER.info("bucketName is empty {}", bucketName.isEmpty());
			LOGGER.info("CSV File is empty {}", csvFile.isEmpty());

			// Get the BlobId for the object we want to create
			// BlobId blobId = BlobId.of(BUCKET_NAME, OBJECT_NAME);

			BlobId blobId = BlobId.of(bucketName, objectName);

			LOGGER.info("blobid name {}", blobId.getName());

			// Create a BlobInfo object with the BlobId and set the content type
			// BlobInfo blobInfo =
			// BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
			/*
			 * BlobInfo blobInfo =
			 * BlobInfo.newBuilder(blobId).setContentType("application/octet-stream") // Set
			 * the // content type // of the file .setContentEncoding("gzip") // Set the
			 * content encoding if applicable .setMetadata(ImmutableMap.of("key1", "value1",
			 * "key2", "value2")) // Set any // metadata for the blob if needed .build();
			 */
			LOGGER.info("blobInfo blobid {}", blobInfo.getBlobId());
			LOGGER.info("blobInfo bucket name {}", blobInfo.getBucket());
			LOGGER.info("blobInfo content type {}", blobInfo.getContentType());
			LOGGER.info("blobInfo size {}", blobInfo.getSize());
			LOGGER.info("blobInfo is Directory {}", blobInfo.isDirectory());

			LOGGER.info("bucketName {}", bucketName);
			LOGGER.info("path of file {}", Paths.get(filePath));
			LOGGER.info("path of file {}", Paths.get(filePath).getFileName());
			LOGGER.info("path of file {}", Paths.get(filePath).getFileSystem());
			LOGGER.info("path of file {}", Paths.get(filePath).toAbsolutePath());

			// com.google.cloud.storage.Bucket bucket = storage.get(bucketName);
			// LOGGER.info("bucket name from storage {}", bucket);
			ClassLoader classLoader1 = getClass().getClassLoader();
			InputStream inputStream = classLoader1.getResourceAsStream(filePath);

			byte[] fileBytes = readBytes(inputStream);

			// byte[] fileBytes = readBytesFromFile("example.csv");
			LOGGER.info("fileBytes  {}", fileBytes.length);

			// Read the file to be uploaded into a byte array
			// byte[] fileBytes = readFileToBytes(FILE_PATH);

			// Create the Blob and upload the file
			Blob blob = storage.create(blobInfo, fileBytes);

			System.out.printf("File %s uploaded to bucket %s as %s%n", filePath, bucketName, blob.getName());

			// Upload the file to the bucket
			// Blob blob = bucket.create("purgeCSV/purge-csv.csv", fileBytes);

			// System.out.println("File uploaded successfully: " + blob.getName());

			// *************************************************************************************************************

			// Optional: set a generation-match precondition to avoid potential race
			// conditions and data corruptions. The request returns a 412 error if the
			// preconditions are not met.
			Storage.BlobWriteOption precondition;
			if (storage.get(bucketName, csvFile) == null) {
				// For a target object that does not yet exist, set the DoesNotExist
				// precondition.
				// This will cause the request to fail if the object is created before the
				// request runs.
				precondition = Storage.BlobWriteOption.doesNotExist();
			} else {
				// If the destination already exists in your bucket, instead set a
				// generation-match
				// precondition. This will cause the request to fail if the existing object's
				// generation
				// changes before the request runs.
				precondition = Storage.BlobWriteOption
						.generationMatch(storage.get(bucketName, objectName).getGeneration());
			}

			List<Path> result;
			String jarPath1 = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			LOGGER.info("jarPath1 {}", jarPath1);

			URI uri = URI.create("jar:file:" + jarPath1);

			LOGGER.info("uri {}", uri);

			try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
				result = Files.walk(fs.getPath("example.csv")).filter(Files::isRegularFile)
						.collect(Collectors.toList());
			}

			LOGGER.info("result {}", result);
			LOGGER.info("result size {}", result.size());
			Path path = result.get(0);
			LOGGER.info("path samsu 1-- {}", path);

			// ****************************************************
			Path fileInJarPathObj = null;
			ClassLoader classLoader = getClass().getClassLoader();
			URL resourceUrl = classLoader.getResource("example.csv");
			if (resourceUrl != null && resourceUrl.getProtocol().equals("jar")) {
				String jarPath = resourceUrl.getPath().substring(5, resourceUrl.getPath().indexOf("!"));
				try (JarFile jarFile = new JarFile(URLDecoder.decode(jarPath, "UTF-8"))) {
					String fileInJarPath = resourceUrl.getPath().substring(resourceUrl.getPath().indexOf("!") + 2);
					JarEntry jarEntry = jarFile.getJarEntry(fileInJarPath);
					String fileInJarAbsolutePath = jarFile.getName() + "!/" + fileInJarPath;
					fileInJarPathObj = Paths.get(fileInJarAbsolutePath);
					LOGGER.info("File path inside jar: {}", fileInJarPathObj);
				} catch (IOException e) {
					LOGGER.error("Failed to read file path inside jar", e);
				}
			}
			// *********************************************************************

			// storage.createFrom(blobInfo, fileInJarPathObj, precondition);

			// System.out.println("path of fifileInJarPathObjle ==" +
			// Paths.get("C:/Users/VIRENDERSINGH/Virender/example.csv"));
			// storage.createFrom(blobInfo,
			// Paths.get(getClass().getResource("/example.csv").toURI()), precondition);
			// storage.create(blobInfo, fileBytes, precondition);
			System.out.println("File " + csvFile + " uploaded to bucket " + bucketName + " as " + csvFile);

			// *****************************************************************************************************************
			// Get WriteChannel from BlobId
			/*
			 * WriteChannel writeChannel = storage.writer(blobId);
			 * 
			 * 
			 * try (OutputStream outputStream = storage.writer(blobInfo); BufferedWriter
			 * bufferedWriter = new BufferedWriter( Channels.newWriter((WritableByteChannel)
			 * outputStream, "UTF-8"))) { FileReader fileReader = new FileReader(csvFile);
			 * BufferedReader bufferedReader = new BufferedReader(fileReader); String line;
			 * // while ((line = bufferedReader.readLine()) != null) { //
			 * bufferedWriter.write(line); // bufferedWriter.newLine(); // }
			 * bufferedReader.close();
			 * System.out.println("CSV file uploaded to GCS bucket successfully.");
			 */

		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	// Create and return a GCS Storage client
	private static Storage getStorageClient() throws IOException {
		// FileInputStream credentialsStream = new
		// FileInputStream("D:/Code/gcp-mig-clser-uat-odm.json"); // Replace with
		// path to

		InputStream credentialsStream = uploadCSV.class.getClassLoader()
				.getResourceAsStream("gcp-mig-clser-uat-odm.json");
		// your GCS credentials
		// file
		LOGGER.info("credentialsStream {}", credentialsStream);
		StorageOptions options = StorageOptions.newBuilder()
				.setCredentials(ServiceAccountCredentials.fromStream(credentialsStream)).build();
		return options.getService();
	}

	private byte[] readBytesFromFile(String filePath) throws IOException {
		LOGGER.info("filePath {}", filePath);

		File file = new File(filePath);
		LOGGER.info("file {}", file);
		byte[] fileBytes = new byte[(int) file.length()];
		// try (FileInputStream fileInputStream = new FileInputStream(file)) {
		try (FileInputStream fileInputStream = new FileInputStream(file)) {
			fileInputStream.read(fileBytes);
		}
		return fileBytes;
	}

	public static byte[] readBytes(InputStream inputStream) throws IOException {
		try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
			byte[] buffer = new byte[4096];
			int bytesRead;
			while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
				byteArrayOutputStream.write(buffer, 0, bytesRead);
			}
			return byteArrayOutputStream.toByteArray();
		}
	}
}
