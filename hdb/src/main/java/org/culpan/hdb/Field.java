package org.culpan.hdb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Field {
    public final static int FIELD_DESCRIPTOR_LENGTH = 64;

    public enum DataType { character, date, floating_point, logical, memo, numeric, long_type, double_type }

    int columnId;

    String name;

    DataType dataType;

    int lenth;

    int decimalCount;

    boolean indexed;

    public static Field createCharacter(int columnId, String name, int lenth) {
        return new Field(columnId, name, DataType.character, lenth, (byte)0, false);
    }

    public static Field createDate(int columnId, String name) {
        return new Field(columnId, name, DataType.date, 8, 0, false);
    }

    public static Field createFloat(int columnId, String name, int lenth, int decimalCount) {
        return new Field(columnId, name, DataType.floating_point, lenth, decimalCount, false);
    }

    public static Field createLogical(int columnId, String name) {
        return new Field(columnId, name, DataType.logical, 1, 0, false);
    }

    public static Field createMemo(int columnId, String name) {
        return new Field(columnId, name, DataType.memo, 4, 0, false);
    }

    public static Field createNumeric(int columnId, String name, int lenth) {
        return new Field(columnId, name, DataType.numeric, lenth, 0, false);
    }

    public static Field createLongType(int columnId, String name) {
        return new Field(columnId, name, DataType.long_type, Long.BYTES, 0, false);
    }

    public static Field createDoubleType(int columnId, String name) {
        return new Field(columnId, name, DataType.double_type, Double.BYTES, 0, false);
    }

    private Field(int columnId, String name, DataType dataType, int lenth, int decimalCount, boolean indexed) {
        this.columnId = columnId;
        this.name = name;
        this.dataType = dataType;
        this.lenth = lenth;
        this.decimalCount = decimalCount;
        this.indexed = indexed;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public int getLenth() {
        return lenth;
    }

    public void setLenth(int lenth) {
        this.lenth = lenth;
    }

    public int getDecimalCount() {
        return decimalCount;
    }

    public void setDecimalCount(int decimalCount) {
        this.decimalCount = decimalCount;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    protected void encodeMetadata(RandomAccessFile fileData) throws IOException  {
        fileData.write(String.format("%-32s", name).getBytes());
        fileData.writeByte(dataType.ordinal());
        fileData.writeShort(lenth);
        fileData.writeShort(decimalCount);
        fileData.writeBoolean(indexed);
        for (int i = 0; i < 26; i++) {
            fileData.writeByte(0);
        }
    }

    protected static Field decodeMetadata(RandomAccessFile fileData, int columnId) throws IOException {
        byte [] lname = new byte[32];
        fileData.read(lname);
        String tname = new String(lname).trim();
        byte ltype = fileData.readByte();
        DataType type = DataType.values()[ltype];
        int length = fileData.readShort();
        int decimalCount = fileData.readShort();
        boolean indexed = fileData.readBoolean();

        Field result = null;
        switch (type) {
            case character:
                result = Field.createCharacter(columnId, tname, length);
                break;
            case date:
                result = Field.createDate(columnId, tname);
                break;
            case floating_point:
                result = Field.createFloat(columnId, tname, length, decimalCount);
                break;
            case logical:
                result = Field.createLogical(columnId, tname);
                break;
            case memo:
                result = Field.createMemo(columnId, tname);
                break;
            case numeric:
                result = Field.createNumeric(columnId, tname, length);
                break;
            case long_type:
                result = Field.createLongType(columnId, tname);
                break;
            case double_type:
                result = Field.createDoubleType(columnId, tname);
                break;
            default:
                throw new RuntimeException("Unrecognized field type");
        }

        fileData.skipBytes(26);
        return result;
    }

    final static Pattern numericPattern = Pattern.compile("\\d+");

    public boolean validate(Object o) {
        boolean result = false;

        switch (dataType) {
            case numeric:
                Matcher matcher = numericPattern.matcher(o.toString());
                result = matcher.matches();
                break;
            case character:
                result = true;
                break;
        }

        return result;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }
}
