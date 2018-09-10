package org.culpan.hdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HDb {
    String dbDirectory;

    public HDb(String dbDirectory) {
        this.dbDirectory = dbDirectory;
        System.out.println(dbDirectory);
    }

    public Table open(String name) {
        try {
            Table table = new Table(this.dbDirectory, name);
            if (!table.file.exists()) {
                throw new RuntimeException("Table " + name + " does not exist");
            }
            table.openExisting();
            return table;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Table openOrCreate(String name) {
        return new Table(this.dbDirectory, name);
    }

    public Table newOverwrite(String name, Field ... fields) {
        try {
            Table table = new Table(this.dbDirectory, name);
            List<Field> fieldList = new ArrayList<>();
            for (Field field : fields) {
                fieldList.add(field);
            }
            table.openNew(fieldList);
            return table;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
