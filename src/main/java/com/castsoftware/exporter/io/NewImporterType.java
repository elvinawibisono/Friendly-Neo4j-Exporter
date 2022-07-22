package com.castsoftware.exporter.io;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.deserialize.NodeDeserializer;
import com.castsoftware.exporter.deserialize.RelationshipDeserializer;
import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.exceptions.file.FileCorruptedException;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.results.OutputMessage;
import com.castsoftware.exporter.utils.Shared;
import com.opencsv.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.neo4j.cypher.internal.NeedsReplan;


public class NewImporterType {

    private static final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString(); // .csv
	private static final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString(); // relationship
	private static final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString(); // node


	private Neo4jAl neo4jAl;
	private String delimiter;

    /**
     * 
     * Process Nodes in a 2D Array 
     * @param entries 
     * 
     */

    private void processNode(ZipFile zf, List<ZipEntry> entries) {
		for(ZipEntry en : entries)  { // Loop over the entries
			try (InputStreamReader br = new InputStreamReader(zf.getInputStream(en), "UTF-8")) {

				RFC4180Parser  parser = new RFC4180ParserBuilder()
						.withSeparator(CSVWriter.DEFAULT_SEPARATOR)
						.withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
						.build();

				CSVReader reader = new CSVReaderBuilder(br)
						.withCSVParser(parser)
						.build();

				String[] headers = reader.readNext(); // Read headers
				List<String> lHeaders = List.of(headers);

                List<String> newHeaders = new ArrayList<>(); 

                //get the headers of the CSV file without the blank cell 
                for (int i = 1; i<lHeaders.size(); i++){

                    newHeaders.add(lHeaders.get(i));

                }

				neo4jAl.info(String.format("Header discovered for %s : %s ", zf.getName(), String.join(", ", newHeaders)));
				neo4jAl.info(String.valueOf(lHeaders.size())); 

				/* 
				//get the list of values of the headers 
				List<String>values = new ArrayList<>(); 

				for(int i =0; i<lHeaders.size(); i++){

					values.add(Shared.NODE_PROPERTY_TYPE);
				}

				neo4jAl.info(values.toString()); 
				neo4jAl.info(String.valueOf(values.size())); 

				*/

				
				 
				// Parse and create node
				int success = 0;
				int errors = 0;

				//Neo4jAlUtils.getNodeType(neo4jAl, newHeaders)

               
                 try{

                    NodeDeserializer.mergeNodeType(neo4jAl,newHeaders);
                    success++; 

                } catch (Neo4jQueryException e) {
						errors++;
				}
                
                neo4jAl.info(String.format("File %s was imported. %d Nodes created, %d errors.",  en.getName(), success, errors));

              

			} catch (Exception e) {
				neo4jAl.error(String.format("An error occurred trying to process zip entry '%s'.", en.getName()), e);
			}
			
        }
		

	}




	/**
	 * Process relationship
	 * @param entries List of Relationship Entries
	 */
	
	private void processRelationships(ZipFile zf, List<ZipEntry> entries) {
		for(ZipEntry en : entries)  { // Loop over the entries
			try (InputStreamReader br = new InputStreamReader(zf.getInputStream(en), "UTF-8")) {

				// Read header
				RFC4180Parser  parser = new RFC4180ParserBuilder()
						.withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
						.withSeparator(CSVWriter.DEFAULT_SEPARATOR)
						.build();

				CSVReader reader = new CSVReaderBuilder(br)
						.withCSVParser(parser)
						.build();

				String[] headers = reader.readNext(); // Read headers
				List<String> lHeaders = List.of(headers);

				//get the headers without the first blank cell 
				List<String> newHeaders = new ArrayList<>(); 

				for (int i = 1; i<lHeaders.size(); i++){

					newHeaders.add(lHeaders.get(i)); 

				}

				// Parse and create node
				String[] record;
				List<String> elements;
				int success = 0;
				int errors = 0;
				while ((record = reader.readNext()) != null) {
					elements = List.of(record);

					/* 
					neo4jAl.info(lHeaders.toString()); 
					neo4jAl.info(String.valueOf(lHeaders.size())); 
					neo4jAl.info(elements.toString());
					neo4jAl.info(String.valueOf(elements.size()));  

					*/

					
					try {
						RelationshipDeserializer.mergeRelationshipType(neo4jAl, lHeaders, elements);
						success++;
					} catch (Neo4jQueryException e) {
						errors++;
					}

					
				}

				neo4jAl.info(String.format("File %s was imported. %d Relationships created, %d errors.",  en.getName(), success, errors));

			} catch (Exception e) {
				neo4jAl.error(String.format("An error occurred trying to process zip entry '%s'.", en.getName()), e);
			}
		}
	}


    /**
	 * Parse a zip file
	 * @param file File to process
	 * @throws IOException
	 * @throws Neo4jQueryException
	 */
	private void parseZip(File file) throws Exception {

		List<ZipEntry> nodesMap = new ArrayList<>();
		List<ZipEntry> relationshipMap = new ArrayList<>();

		try (ZipFile zf = new ZipFile(file)) {
			Enumeration entries = zf.entries();

			// Classify the entry
			while (entries.hasMoreElements()) {
				// Get the entry
				ZipEntry ze = (ZipEntry) entries.nextElement();
				String filename = ze.getName();

				// Classify the entry
				if (ze.getSize() < 0) continue; // Empty entry
				if(filename.startsWith(NODE_PREFIX)) {
					nodesMap.add(ze);
				} else if (filename.startsWith(RELATIONSHIP_PREFIX)) {
					relationshipMap.add(ze);
				} else {
					neo4jAl.error(String.format("Filename '%s' is a poorly formatted and has been ignored.", filename));
				}
			}

			// Process nodes
			processNode(zf, nodesMap);

			// Process nodes
			processRelationships(zf, nodesMap);

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to open the zip file at '%s'.", file.getName()), e);
			throw new Exception("Failed to open the zip file.");
		}
	}


    	/**
	 * Load and process the node from a Zip File
	 * @param path Path to the zip file
	 * @return
	 * @throws Exception
	 */
	public void load(String path) throws Exception {
		Path zipPath = Path.of(path);

		// Check the existence of the Zip File
		if(!Files.exists(zipPath)) {
			throw new Exception(String.format("No zip file found at '%s'.", path));
		}

		// Open and parse the zip file
		parseZip(zipPath.toFile());

	}


	/**
	 * Exporter
	 * @param neo4jAl
	 */
	public NewImporterType(Neo4jAl neo4jAl, String delimiter) {
		this.neo4jAl = neo4jAl;
		this.delimiter = delimiter;
	}

	

    
}
