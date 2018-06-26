package db;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

public class Table {

    private Map<String, Column> colNames;

    public Table(Column[] cols) {
        colNames = new LinkedHashMap<>();
        for (Column c : cols) {
            colNames.put(c.getName(), c);
        }
    }

    String addRow(List row) {
        Iterator c = colNames.values().iterator();
        String result = "";
        int index = row.size();
        if (index != getNumCols()) {
            return "ERROR: incorrect row size.";
        }
        for (int i = 0; i < index; i++) {
            result = ((Column) c.next()).addLast(String.valueOf(row.get(i)));
            if (result.contains("ERROR:")) {
                if (i == 0) {
                    return result;
                } else {
                    Iterator c2 = colNames.values().iterator();
                    for (int j = 0; j < i; j++) {
                        Column col = (Column) c2.next();
                        col.remove(col.size() - 1);
                        return result;
                    }
                }
            }
        }
        return result;
    }

    public List getRow(int index) {
        Iterator c = colNames.keySet().iterator();
        List row = new ArrayList();
        while (c.hasNext()) {
            row.add(getColumn((String) c.next()).getItem(index));
        }
        return row;
    }

    void removeRow(int r) {
        Iterator c = colNames.values().iterator();
        int s = getRow(r).size();
        for (int i = 0; i < s; i++) {
            ((Column) c.next()).remove(r);
        }
    }

    Set<String> getColNames() {
        return colNames.keySet();
    }

    Column getColumn(String name) {
        return colNames.get(name);
    }

    int size() {
        return colNames.values().iterator().next().size();
    }

    int getNumCols() {
        return colNames.keySet().size();
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (Column c : colNames.values()) {
            s.append(c.getFullName() + ",");
        }
        s.deleteCharAt(s.length() - 1);
        s.append("\n");

        int colLength = ((colNames.values().iterator().next())).size();
        for (int i = 0; i < colLength; i++) {
            for (Column c : colNames.values()) {
                if (c.getColType().equals("float") && !c.getItem(i).equals("NOVALUE")
                        && !c.getItem(i).equals("NaN")) {
                    s.append(String.format("%.3f", c.getItem(i)) + ",");
                } else {
                    s.append(c.getItem(i) + ",");
                }
            }
            s.deleteCharAt(s.length() - 1);
            s.append("\n");
        }
        return s.toString();
    }
}
