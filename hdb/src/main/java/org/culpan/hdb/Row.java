package org.culpan.hdb;

import java.util.ArrayList;
import java.util.List;

public class Row {
    List<Field> fields;

    List<Object> data;

    long rowid = Long.MAX_VALUE;

    protected Row(List<Field> fields) {
        this.fields = fields;
        this.data = new ArrayList<>();
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<Object> getData() {
        return data;
    }

    public long getRowid() {
        return rowid;
    }

    public boolean isUpdateable() {
        return rowid != Long.MAX_VALUE;
    }

    public Field getFieldMetadata(int index) {
        if (index >= fields.size() || index < 0) return null;

        return fields.get(index);
    }

    public Field getFieldMetadata(String name) {
        Field result = null;

        for (Field field : fields) {
            if (field.getName().equalsIgnoreCase(name)) {
                result = field;
                break;
            }
        }

        return result;
    }

    public Object getField(int index) {
        if (index >= data.size() || index < 0) return null;

        return data.get(index);
    }

    public Object getField(String name) {
        Field field = getFieldMetadata(name);
        if (field != null) {
            return data.get(field.getColumnId());
        }

        return null;
    }

    public void setField(int index, Object value) {
        if (index >= data.size() || index < 0) return;

        data.set(index, value);
    }

    public void setField(String name, Object value) {
        Field field = getFieldMetadata(name);
        if (field != null) {
            setField(field.getColumnId(), value);
        }
    }
}
