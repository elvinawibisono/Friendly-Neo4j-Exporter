package com.castsoftware.exporter.io;

import com.castsoftware.exporter.csv.Formatter;
import com.castsoftware.exporter.csv.NodeRecord;
import com.castsoftware.exporter.csv.RelationshipRecord;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.file.FileIOException;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.NodesUtils;
import com.castsoftware.exporter.utils.RelationshipsUtils;
import com.opencsv.*;
import org.neo4j.graphdb.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class NewExporter {

	private static final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString(); // .csv
	private static final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString(); // relationship
	private static final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString(); // node


	private final Neo4jAl neo4jAl;
	private String delimiter;
	private Boolean considerNeighbors;

	/**
	 * Appends all the files created during this process to the target zip.
	 * Every file appended will be remove once added to the zip.
	 * @param targetName Name of the ZipFile
	 * @throws IOException
	 */
	private Path createZip(Path path, String targetName, List<Path> toZipFiles) throws FileIOException {
		// Create the zip file
		String filename = targetName.concat(".zip");
		Path filepath = path.resolve(filename);
		File f =  filepath.toFile();
		this.neo4jAl.info(String.format("Creating zip file at '%s'..", filepath.toString()));

		try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(f))) {

			for(Path toZipFilePath : toZipFiles) {
				File fileToZip = toZipFilePath.toFile();
				String nameToZip = toZipFilePath.getFileName().toString();

				try (FileInputStream fileStream = new FileInputStream(fileToZip)){
					ZipEntry e = new ZipEntry(nameToZip);
					zipOut.putNextEntry(e);

					byte[] bytes = new byte[1024];
					int length;
					while((length = fileStream.read(bytes)) >= 0) {
						zipOut.write(bytes, 0, length);
					}
				} catch (Exception e) {
					this.neo4jAl.error("An error occurred trying to zip file with name : ".concat(nameToZip), e);
				}

				if(!fileToZip.delete()) this.neo4jAl.error("Error trying to delete file with name : ".concat(nameToZip));
			}

			return filepath;

		} catch (IOException e) {
			this.neo4jAl.error("An error occurred trying create zip file with name : ".concat(targetName), e);
			throw new FileIOException("An error occurred trying create zip file with name.", e, "SAVExCZIP01");
		}

	}

	/**
	 * Export a specific label to a specified path and return the list of files ( node + relationships ) created
	 * @param label Labels to export
	 * @return The list of files created
	 */
	private Path exportLabels(Path path, String label) throws Exception {
		neo4jAl.info(String.format("Start to export label : %s..", label));

		String filename = NODE_PREFIX.concat(label).concat(EXTENSION);
		Path filepath = path.resolve(filename);
		Boolean exists = Files.exists(filepath);

		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){

			// Get the headers
			List<String> headers = NodeRecord.getHeaders(neo4jAl, label);

			char charDel = this.delimiter.isEmpty() ? CSVWriter.DEFAULT_SEPARATOR : this.delimiter.charAt(0);
			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = new CSVWriterBuilder(out)
					.withSeparator(charDel)
					.withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
					.withEscapeChar(CSVWriter.DEFAULT_ESCAPE_CHARACTER)
					.withLineEnd(CSVWriter.DEFAULT_LINE_END);


			try (ICSVWriter printer = builder.build()) {

				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				ResourceIterator<Node> it = neo4jAl.findNodes(Label.label(label));
				Node x;
				int count = 0;
				String[] values;
				for (Iterator<Node> iter = it.stream().iterator(); iter.hasNext(); ) {
					x = iter.next();
					values = Formatter.toString(NodeRecord.getNodeRecord(neo4jAl, x, headers));
					printer.writeNext(values);

					// Count
					count++;
					if(count % 200 == 0) neo4jAl.info(String.format("%d labels exported...", count));
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of nodes with labels '%s'.", label, e));
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the label '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		}
	}

	/**
	 * Export the relationships
	 * @param neo4jAl Neo4j Access Layer
	 * @param path Path of export folder
	 * @param type Type to export
	 * @return
	 */
	private Path exportRelationships(Neo4jAl neo4jAl, Path path, String type, String label) throws Exception {
		neo4jAl.info(String.format("Start to export relationship : %s..", type));

		String filename = RELATIONSHIP_PREFIX.concat(type).concat(EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){

			// Get the headers
			List<String> headers = RelationshipRecord.getHeaders(neo4jAl, type);

			char charDel = this.delimiter.isEmpty() ? CSVWriter.DEFAULT_SEPARATOR : this.delimiter.charAt(0);
			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = new CSVWriterBuilder(out)
					.withSeparator(charDel)
					.withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
					.withEscapeChar(CSVWriter.DEFAULT_ESCAPE_CHARACTER)
					.withLineEnd(CSVWriter.DEFAULT_LINE_END);

			try (ICSVWriter printer = builder.build()) {

				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				ResourceIterator<Node> it = neo4jAl.findNodes(Label.label(label));
				int count = 0;
				Node x;
				String[] values;
				for (Iterator<Node> iter = it.stream().iterator(); iter.hasNext(); ) {
					x = iter.next();
					for(Relationship rel : RelationshipsUtils.getRelationships(x, type, Direction.BOTH)) {
						values = Formatter.toString(RelationshipRecord.getRelationshipRecord(neo4jAl, rel, headers));
						printer.writeNext(values);

						// Count
						count++;
						if(count % 200 == 0) neo4jAl.info(String.format("%d relationships exported...", count));
					}
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of nodes with labels '%s'.", label, e));
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the relationship '%s'.", type), e);
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		}

	}

	/**
	 * Export the list of relationships
	 * @param neo4jAl Neo4j Access Layer
	 * @param labels Labels to process
	 * @return
	 */
	private List<Path> exportRelationships(Neo4jAl neo4jAl, Path directory, List<String> labels) throws Exception {
		List<Path> paths = new ArrayList<>();
		Map<String, List<String>> headersMap = new HashMap<>();
		Path temp = null;
		// For each labels, export the related type of relationships
		for(String label : labels) {
			// Get the list
			List<String> relationshipTypes = RelationshipsUtils.getRelationshipTypeForLabel(neo4jAl, label);
			neo4jAl.info(String.format("%d relationships identified for label '%s'. Relationships: [ %s ]",
					relationshipTypes.size(), label, String.join(", ", relationshipTypes)));

			for(String type : relationshipTypes) {
				temp = this.exportRelationships(neo4jAl, directory, type, label);
				paths.add(temp);
			}
		}

		return paths;
	}

	/**
	 * Export the list of labels
	 * @param path Path to export
	 * @param fileName File name to export
	 */
	public Path export(String path, String fileName, List<String> labels) throws Exception, FileIOException {
		neo4jAl.info("Verifying path and authorization...");
		Path directoryPath = Paths.get(path);
		if( !Files.exists(directoryPath)) throw new Exception(String.format("The specified directory doesn't exist. Path : '%s'.", path)); // If exists
		if(!Files.isWritable(directoryPath)) throw new Exception(String.format("Not enough authorization to write files: Path : '%s'.", path)); // If Writable
		neo4jAl.info("Path verified and writable");

		// For each label create, process and create files ( nodes + labels )
		neo4jAl.info("Exporting labels...");
		List<Path> createdFiles = new ArrayList<>();
		for(String label : labels) {
			createdFiles.add(this.exportLabels(directoryPath, label));
		}
		neo4jAl.info("Labels exported.");

		// For each label, export the list of relationships
		neo4jAl.info("Exporting relationships...");
		createdFiles.addAll(this.exportRelationships(neo4jAl, directoryPath, labels));
		neo4jAl.info("Relationships exported.");

		// From the list of files created, zip them
		neo4jAl.info("Zipping the files created...");
		return this.createZip(directoryPath, fileName, createdFiles);
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void setConsiderNeighbors(Boolean considerNeighbors) {
		this.considerNeighbors = considerNeighbors;
	}

	/**
	 * Exporter
	 * @param neo4jAl
	 */
	public NewExporter(Neo4jAl neo4jAl, String delimiter) {
		this.neo4jAl = neo4jAl;
		this.delimiter = delimiter;
	}
}
