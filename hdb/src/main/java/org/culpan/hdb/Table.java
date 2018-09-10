package org.culpan.hdb;

import org.culpan.hdb.Utils.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class Table {
    public final static int HEADER_LENGTH = 128;

    File file;

    String name;

    Date lastUpdate;

    long numberRecords;

    long numberDeletedRecords;

    long firstDeleted = Long.MAX_VALUE;

    int lengthOfHeader;

    int lengthOfRecord;

    List<Field> fields;

    RandomAccessFile fileData;

    String lastErrorMessage;

    long lastUpdateRowId;

    protected Table(String directory, String name) {
        this.name = name;
        if (name.toLowerCase().endsWith(".dbh")) {
            this.file = new File(directory, name);
        } else {
            this.file = new File(directory, name + ".dbh");
        }
    }

    /**
     * For unit testing purposes only
     */
    protected Table() {

    }

    protected void openNew(List<Field> fields) throws IOException {
        if (file.exists()) {
            file.delete();
        }

        this.fields = fields;

        lengthOfHeader = HEADER_LENGTH + (fields.size() * Field.FIELD_DESCRIPTOR_LENGTH);
        lengthOfRecord = fields.stream().mapToInt(Field::getLenth).sum() + 1;
        lastUpdate = new Date();
        numberRecords = 0;
        fileData = new RandomAccessFile(file, "rws");
        writeHeader();
        writeFields();
    }

    private void writeFields() throws IOException {
        for (Field field : fields) {
            field.encodeMetadata(fileData);
        }
    }

    private void writeHeader() throws IOException {
        fileData.write(0);
        fileData.writeLong(lastUpdate.getTime());
        fileData.writeLong(numberRecords);
        fileData.writeLong(numberDeletedRecords);
        fileData.writeLong(firstDeleted);
        fileData.writeInt(lengthOfHeader);
        fileData.writeInt(lengthOfRecord);
        fileData.writeInt(fields.size());
        for (int i = 0; i < 83; i++) {
            fileData.writeByte(0);
        }
    }

    private void readHeader() throws IOException {
        fileData.skipBytes(1);   // file id
        lastUpdate = new Date(fileData.readLong());
        numberRecords = fileData.readLong();
        numberDeletedRecords = fileData.readLong();
        firstDeleted = fileData.readLong();
        lengthOfHeader = fileData.readInt();
        lengthOfRecord = fileData.readInt();
        int numFields = fileData.readInt();
        fileData.skipBytes(83);

        fields = new ArrayList<>();
        // Read each field
        for (int i = 0; i < numFields; i++) {
            fields.add(Field.decodeMetadata(fileData, i));
        }
    }

    public boolean saveOrUpdateRow(Row row) {
        if (row.isUpdateable()) {
            return updateRecord(row.getRowid(), row.data);
        } else {
            return insertRecord(row.data);
        }
    }

    public boolean updateRecord(long rowIndex, List<Object> data) {
        if (data.size() != fields.size()) {
            throw new RuntimeException((data.size() < fields.size() ? "Too few" : "Too many") + " data for row");
        }

        byte [] buffer = null;

        try {
            buffer = buildWriteBuffer(data);

            if (buffer != null) {
                fileData.seek(lengthOfHeader + (lengthOfRecord * rowIndex));
                lastUpdateRowId = rowIndex;

                fileData.write(buffer);
                lastUpdate = new Date();
                updateHeaderInfo();
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return buffer != null;
    }

    public boolean insertRecord(Object ... data) {
        List<Object> objects = new ArrayList<>();
        for (Object o : data) {
            objects.add(o);
        }
        return insertRecord(objects);
    }

    protected byte [] buildWriteBuffer(List<Object> data) {
        byte [] buffer = new byte[lengthOfRecord];
        int index = 0;
        int bufferPtr = 1;
        buffer[0] = 0;   // deleted flag
        for (Field field : fields) {
            if (field.getDataType() == Field.DataType.logical) {
                Object o = data.get(index++);
                if (o instanceof Boolean) {
                    if (((Boolean)o).booleanValue()) {
                        buffer[bufferPtr++] = 1;
                    } else {
                        buffer[bufferPtr++] = 0;
                    }
                } else if (o instanceof String) {
                    Boolean value = Boolean.valueOf(o.toString());
                    if (value) {
                        buffer[bufferPtr++] = 1;
                    } else {
                        buffer[bufferPtr++] = 0;
                    }
                } else {
                    lastErrorMessage = "'" + o.toString() + "' for field '" + field.getName() + "' is invalid for type " + field.getDataType().toString();
                    buffer = null;
                    break;
                }
            } else if (field.getDataType() == Field.DataType.long_type) {
                Object o = data.get(index++);
                if (o instanceof Number) {
                    byte[] barray = Utils.longToBytes(((Number) o).longValue());
                    System.arraycopy(barray, 0, buffer, bufferPtr, Long.BYTES);
                    bufferPtr += Long.BYTES;
                } else {
                    lastErrorMessage = "'" + o.toString() + "' for field '" + field.getName() + "' is invalid for type " + field.getDataType().toString();
                    buffer = null;
                    break;
                }
            } else if (field.getDataType() == Field.DataType.date) {
                Object o = data.get(index++);
                long value;
                if (o instanceof Date) {
                    value = ((Date)o).getTime();
                } else if (o instanceof Number) {
                    value = ((Number)o).longValue();
                } else {
                    lastErrorMessage = "'" + o.toString() + "' for field '" + field.getName() + "' is invalid for type " + field.getDataType().toString();
                    buffer = null;
                    break;
                }
                byte[] barray = Utils.longToBytes(value);
                System.arraycopy(barray, 0, buffer, bufferPtr, Long.BYTES);
                bufferPtr += Long.BYTES;
            } else if (field.getDataType() == Field.DataType.numeric || field.getDataType() == Field.DataType.character) {
                String value = data.get(index++).toString();
                if (!field.validate(value)) {
                    lastErrorMessage = "'" + value + "' for field '" + field.getName() + "' is invalid for type " + field.getDataType().toString();
                    buffer = null;
                    break;
                } else if (value.length() > field.getLenth()) {
                    lastErrorMessage = "'" + value + "' for field '" + field.getName() + "' is greater than allowable length of " + field.getLenth();
                    buffer = null;
                    break;
                }
                for (int i = 0; i < field.getLenth(); i++) {
                    if (i < value.length()) {
                        buffer[bufferPtr] = (byte)value.charAt(i);
                    } else {
                        buffer[bufferPtr] = 0;
                    }
                    bufferPtr++;
                }
            }
        }

        return buffer;
    }

    public boolean insertRecord(List<Object> data) {
        if (data.size() != fields.size()) {
            throw new RuntimeException((data.size() < fields.size() ? "Too few" : "Too many") + " data for row");
        }

        byte [] buffer = null;

        try {
            buffer = buildWriteBuffer(data);

            if (buffer != null) {
                if (numberDeletedRecords > 0 && firstDeleted != Long.MAX_VALUE) {
                    fileData.seek(lengthOfHeader + (lengthOfRecord * firstDeleted) + 1);
                    long newFirstDeleted = fileData.readLong();
                    fileData.seek(lengthOfHeader + (lengthOfRecord * firstDeleted));
                    lastUpdateRowId = firstDeleted;
                    firstDeleted = newFirstDeleted;
                    numberDeletedRecords--;
                } else {
                    fileData.seek(lengthOfHeader + (lengthOfRecord * numberRecords));
                    lastUpdateRowId = numberRecords;
                    numberRecords++;
                }

                fileData.write(buffer);
                lastUpdate = new Date();
                updateHeaderInfo();
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return buffer != null;
    }

    public List<Row> getAllRows() {
        List<Row> result = new ArrayList<>();
        for (int i = 0; i < getNumberRecords(); i++) {
            Row row = getRow(i);
            if (row != null) result.add(row);
        }
        return result;
    }

    public void deleteRow(int rowIndex) throws IOException {
        if (rowIndex >= numberRecords) return;

        fileData.seek(lengthOfHeader + (lengthOfRecord * rowIndex));
        if (fileData.readByte() > 0) return;  // already marked as deleted
        fileData.seek(lengthOfHeader + (lengthOfRecord * rowIndex));
        fileData.writeByte(1); // mark as deleted
        fileData.writeLong(firstDeleted);

        byte[] buffer = new byte[lengthOfRecord - 9];
        for (int i = 1; i < buffer.length; i++) {
            buffer[i] = 0;
        }
        lastUpdateRowId = rowIndex;
        fileData.write(buffer);
        numberDeletedRecords++;
        firstDeleted = rowIndex;
        lastUpdate = new Date();
        updateHeaderInfo();
    }

    public List<Row> getRows(RowFilter rowFilter) {
        List<Row> rows = getAllRows();
        Iterator<Row> iRow = rows.iterator();
        while (iRow.hasNext()) {
            Row row = iRow.next();
            if (!rowFilter.matches(row)) {
                iRow.remove();
            }
        }
        return rows;
    }

    public Row getRow(int rowIndex) {
        if (rowIndex >= numberRecords) return null;

        Row result = new Row(fields);
        result.rowid = rowIndex;
        try {
            fileData.seek(lengthOfHeader + (lengthOfRecord * rowIndex));
            byte[] buffer = new byte[lengthOfRecord];
            fileData.read(buffer);
            if (buffer[0] != 0) { // deleted record
                return null;
            }

            int bufferPtr = 1;
            for (Field field : fields) {
                if (field.getDataType() == Field.DataType.long_type) {
                    long value = Utils.bytesToLong(buffer, bufferPtr);
                    result.getData().add(value);
                    bufferPtr += Long.BYTES;
                } else if (field.getDataType() == Field.DataType.logical) {
                    boolean value = (buffer[bufferPtr++] == 1);
                    result.getData().add(value);
                } else if (field.getDataType() == Field.DataType.date) {
                    long value = Utils.bytesToLong(buffer, bufferPtr);
                    result.getData().add(new Date(value));
                    bufferPtr += Long.BYTES;
                } else if (field.getDataType() == Field.DataType.numeric || field.getDataType() == Field.DataType.character) {
                    String s = "";
                    for (int i = 0; i < field.getLenth(); i++) {
                        char c = (char)buffer[bufferPtr];
                        if (c == 0 && !s.isEmpty()) {
                            if (field.getDataType() == Field.DataType.numeric) {
                                result.getData().add(Long.parseLong(s));
                            } else {
                                result.getData().add(s);
                            }
                            s = "";
                        } else if (c != 0) {
                            s += c;
                        }
                        bufferPtr++;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

    public Row getEmptyRow() {
        Row result = new Row(fields);
        for (int i = 0; i < fields.size(); i++) {
            result.data.add(null);
        }
        return result;
    }

    private void updateHeaderInfo() throws IOException {
        fileData.seek(1);
        fileData.writeLong(lastUpdate.getTime());
        fileData.writeLong(numberRecords);
        fileData.writeLong(numberDeletedRecords);
        fileData.writeLong(firstDeleted);
    }

    protected void openExisting() throws IOException {
        fileData = new RandomAccessFile(file, "rws");
        readHeader();
    }

    public boolean isOpen() {
        return fileData != null;
    }

    public void close() throws IOException {
        fileData.close();
        fileData = null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public long getNumberActiveRecords() {
        return numberRecords - numberDeletedRecords;
    }

    public long getNumberRecords() {
        return numberRecords;
    }

    public void setNumberRecords(long numberRecords) {
        this.numberRecords = numberRecords;
    }

    public int getLengthOfHeader() {
        return lengthOfHeader;
    }

    public void setLengthOfHeader(int lengthOfHeader) {
        this.lengthOfHeader = lengthOfHeader;
    }

    public int getLengthOfRecord() {
        return lengthOfRecord;
    }

    public void setLengthOfRecord(int lengthOfRecord) {
        this.lengthOfRecord = lengthOfRecord;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public long getNumberDeletedRecords() {
        return numberDeletedRecords;
    }

    public void setNumberDeletedRecords(long numberDeletedRecords) {
        this.numberDeletedRecords = numberDeletedRecords;
    }

    public long getFirstDeletedRowId() {
        return firstDeleted;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }
}
