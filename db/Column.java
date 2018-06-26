package db;

import java.util.ArrayList;
import java.util.List;

public class Column<T extends Comparable<T>> {

    private String name;
    private List<T> col;
    private String type;

    public Column(String name, String type) {
        this.name = name;
        this.col = new ArrayList<T>();
        this.type = type;
    }

    String getColType() {
        return type;
    }

    String getName() {
        return name;
    }

    String getFullName() {
        return name + " " + type;
    }

    String addLast(T item) {
        Comparable parseItem = item;

        if (type.equals("int") && !item.equals("NOVALUE") && !item.equals("NaN")) {
            try {
                parseItem = Integer.valueOf((String) item);
            } catch (ClassCastException e) {
                return "ERROR: Wrong item type in column.";
            } catch (NumberFormatException e) {
                return "ERROR: Malformed table";
            }
        } else if (type.equals("float") && !item.equals("NOVALUE") && !item.equals("NaN")) {
            try {
                parseItem = Float.parseFloat((String) item);
            } catch (ClassCastException e) {
                return "ERROR: Wrong item type in column.";
            } catch (NumberFormatException e) {
                return "ERROR: Malformed table";
            }
        } else if (type.equals("string") && !item.equals("NOVALUE") && !item.equals("NaN")) {
            String x = (String) parseItem;
            if (!x.contains("\'")) {
                return "ERROR: Wrong item type in column.";
            }
        }
        col.add((T) parseItem);
        return "";
    }

    T remove(int row) {
        return col.remove(row);
    }

    int size() {
        return col.size();
    }

    @Override
    public String toString() {
        return col.toString();
    }

    T getItem(int row) {
        return col.get(row);
    }

}
