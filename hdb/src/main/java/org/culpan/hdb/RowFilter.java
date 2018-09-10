package org.culpan.hdb;

public interface RowFilter {
    boolean matches(Row row);
}
