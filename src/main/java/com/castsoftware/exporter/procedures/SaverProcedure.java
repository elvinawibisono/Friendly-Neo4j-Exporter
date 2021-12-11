package com.castsoftware.exporter.procedures;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.exceptions.file.FileIOException;
import com.castsoftware.exporter.io.NewExporter;
import com.castsoftware.exporter.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class SaverProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Transaction transaction;

    /**
     * Neo4 Procedure entry point for "fexporter.save()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save(LabelsToSave, Path, ZipFileName, SaveRelationship, ConsiderNeighbors) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. Ex : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. Ex : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). Ex : \"Result_05_09\" " +
     "               - @SaveRelationship - <Boolean> - Save relationships associated to the labels selected. If the option @ConsiderNeighbors is active, relationships involving neighbors' label will also be saved in the process" +
     "               - @ConsiderNeighbors - <Boolean> - Consider the neighbors of selected labels. If a node in the provided label list has a relationship with another node from a different label, this label will also be saved. " +
     "                                                  This option does not necessitate the activation of @SaveRelationship to work, but it is strongly recommended to keep the report consistent." +
     "Example of use : CALL fexporter.save([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", true, true )" +
     "")**/
     @Procedure(value = "fexporter.save", mode = Mode.WRITE)
     public Stream<OutputMessage> saveProcedure(@Name(value = "LabelsToSave") List<String> labelList,
                                                @Name(value = "Path") String path,
                                                @Name(value = "ZipFileName",defaultValue="export") String zipFileName,
                                                @Name(value = "Delimiter", defaultValue=";") String delimiter
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             NewExporter exporter = new NewExporter(neo4jAl, delimiter);
             Path output = exporter.exportLabelList(path, zipFileName, labelList);
             return Stream.of(new OutputMessage(String.format("A new zip file has been created under '%s'.", output.toString())));
         } catch (Exception | FileIOException e) {
             log.error("Failed to export the list of nodes.", e);
             throw new ProcedureException("Failed to export the list of node. Check Neo4J logs for more details...", e);
         }
     }

     public SaverProcedure() { } // Neo4J POJO **/
}
