package org.culpan.trebuchet.cli;

import org.culpan.hdb.Field;
import org.culpan.hdb.HDb;
import org.culpan.hdb.Row;
import org.culpan.hdb.Table;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

public class Main {
    public static void main(String [] args) {
        HDb hDb = new HDb(System.getProperty("user.dir"));
        try {
/*            Table table = hDb.newOverwrite("test",
                    Field.createLogical(0, "living"),
                    Field.createCharacter(1,"firstname", 50),
                    Field.createCharacter(2, "lastname", 50),
                    Field.createLongType(3, "age"),
                    Field.createDate(4, "dob")
                    );*/
            Table table = hDb.open("test");

/*            Row row = table.getEmptyRow();
            row.setField("living", false);
            row.setField("firstname", "Napoleon");
            row.setField("lastname", "Bonaparte");
            row.setField("age", 200);
            Calendar c = Calendar.getInstance();
            c.set(1800, 4, 13);
            row.setField("dob", c.getTime());
            if (!table.saveOrUpdateRow(row)) {
                System.out.println("ERROR: " + table.getLastErrorMessage());
            }*/


//            table.deleteRow(4);

/*            c = Calendar.getInstance();
            c.set(1966, 4, 13);
            table.insertRecord(true, "Harry", "Culpan", 52, c.getTime());
            c.set(1965, 5, 5);
            table.insertRecord(true, "Chris", "Duignan", 53, c.getTime());
            c.set(2008, 11, 9);
            table.insertRecord(true, "Cade", "Parnell", 10, c.getTime());
/*            if (!table.insertRecord("Alice", "Plain", 21)) {
                System.out.println("ERROR: " + table.getLastErrorMessage());
            }*/

            System.out.println("Number of records: " + table.getNumberActiveRecords());
            System.out.println("Number of physical records: " + table.getNumberRecords());
            System.out.println("Number of deleted records: " + table.getNumberDeletedRecords());
            System.out.println("First deleted record: " + table.getFirstDeletedRowId());
/*            List<Row> resultSet = table.getRows(r -> (r.getField("lastname").toString().equals("Bonaparte")));
            if (resultSet.size() == 0) {
                System.out.println("No records found");
            } else if (resultSet.size() > 1) {
                System.out.println("Too many records found: " + resultSet.size());
            } else {
                Row row = resultSet.get(0);
                row.setField("age", 201);
                row.setField("firstname", "Johnny");
                table.saveOrUpdateRow(row);
            }*/

            table.getRows(r -> (((Boolean)r.getField("living")).booleanValue())).forEach(r -> outputRowData(r));

//            table.getAllRows().forEach(r -> outputRowData(r));

/*            List<Field> fields = table.getFields();
            fields.forEach(f -> {
                System.out.println(String.format("%s [type=%s, length=%d]", f.getName(), f.getDataType().toString(), f.getLenth()));
            });*/

            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void outputRowData(Row row) {
        System.out.println("Row " + row.getRowid() + ":");
        for (int i = 0; i < row.getData().size(); i++) {
            Field field = row.getFieldMetadata(i);
            System.out.println(String.format("  %s : %s", field.getName(), row.getField(i).toString()));
        }

    }
}
