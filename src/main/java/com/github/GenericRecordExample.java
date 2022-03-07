package com.github;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExample {

    private static String fileName = "customer-generic.avro";

    public static void main(String[] args) {
        Schema schema = getSchema();
        GenericData.Record customer = getBuilder(schema);
        System.out.println(customer);
        writeToFile(schema, customer);
        readFromFile();
    }

    private static void readFromFile() {
        final File file = new File(fileName);
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){
            customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());
            System.out.println("First name: " + customerRead.get("first_name"));
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile(Schema schema, GenericData.Record customer) {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(fileName));
            dataFileWriter.append(customer);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
    }

    private static GenericData.Record getBuilder(Schema schema) {
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "Vikram");
        customerBuilder.set("last_name", "Singh");
        customerBuilder.set("age", 25);
        customerBuilder.set("height", 23f);
        customerBuilder.set("weight", 80.0f);
        customerBuilder.set("automated_email", false);
        return customerBuilder.build();

    }

    private static Schema getSchema() {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse("{\n" +
            "     \"type\": \"record\",\n" +
            "     \"namespace\": \"com.example\",\n" +
            "     \"name\": \"Customer\",\n" +
            "     \"fields\": [\n" +
            "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
            "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
            "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
            "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
            "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
            "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
            "     ]\n" +
            "}");
    }

}
