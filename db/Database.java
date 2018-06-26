package db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.PrintWriter;
import java.util.NoSuchElementException;

public class Database {

    private Map<String, Table> tableMap;

    public Database() {
        tableMap = new HashMap<>();
    }

    public String transact(String query) {
        return dbParse.parse(this, new String[]{query});
    }

    /* Creates a table with given name and columns. */
    void createTable(String name, Column[] cols) {
        tableMap.put(name, new Table(cols));
    }

    /* Adds the Table object into the database. */
    void createTable(String name, Table table) {
        tableMap.put(name, table);
    }

    /* Loads a table into the database where 'tableName' is the
    ** name of the table file. */
    String load(String tableName) {
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(tableName + ".tbl")));
            String nextLine = br.readLine();
            String[] splitLine = nextLine.split(",");
            Column[] cols = new Column[splitLine.length];

            for (int i = 0; i < splitLine.length; i++) {
                String[] names = splitLine[i].split(" ");
                if (names.length != 2) {
                    return "ERROR: Malformed table header";
                }
                cols[i] = new Column(names[0], names[1]);
            }

            createTable(tableName, cols);
            while ((nextLine = br.readLine()) != null) {
                splitLine = nextLine.split(",");
                List row = new ArrayList();
                for (int i = 0; i < splitLine.length; i++) {
                    row.add(splitLine[i]);
                }
                if (!(row.get(0).equals("") && row.size() == 1)) {
                    result = addRow(tableName, row);
                }
            }
        } catch (FileNotFoundException e) {
            return "ERROR: File: " + tableName + " not found.";
        } catch (NoSuchElementException e) {
            return "ERROR: Invalid file format.";
        } catch (IOException e) {
            return "ERROR: File reading error";
        } catch (NullPointerException e) {
            return "ERROR: Malformed";
        }


        return result;
    }

    /* Stores a table within the database to a .tbl file. */
    String store(String name) {

        try {
            File file = new File(name + ".tbl");
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter buffer = new PrintWriter(file);
            if (tableMap.get(name) == null) {
                return "ERROR: Table not found.";
            }
            buffer.println(print(name));
            buffer.close();

        } catch (FileNotFoundException e) {
            return "ERROR: File not found.";
        } catch (IOException e) {
            return "ERROR: IOException";
        }
        return "";
    }

    /* Drops the table with the given name from the database. */
    String drop(String name) {
        if (!tableMap.keySet().contains(name)) {
            return "ERROR: Table not contained in database.";
        }
        tableMap.remove(name);
        return "";
    }

    /* Inserts the list of values into the database with the given name. */
    String insert(String name, List values) {
        String result = "";
        if (!tableMap.keySet().contains(name)) {
            return "ERROR: Table not contained in database.";
        }
        try {
            result = tableMap.get(name).addRow(values);
        } catch (NoSuchElementException e) {
            return "ERROR: Too many elements in row.";
        } catch (ArrayIndexOutOfBoundsException e) {
            return "ERROR: Too many elements in row.";
        } catch (IllegalArgumentException e) {
            return "ERROR: Incompatible types.";
        }
        return result;
    }

    /* Performs a cartesian join on the set of tables passed in. */
    private Table join(String[] tables) {

        if (tables.length == 1) {
            return tableMap.get(tables[0]);
        }

        Table t = join(tables[0], tables[1]);
        createTable("$join", t);

        for (int i = 2; i < tables.length; i++) {
            t = join("$join", tables[i]);
        }
        return t;
    }

    /* Combines two tables by merging pairs of rows. Rows are only merged if all
    ** of their shared values are equal.
    ** Pre-condition: f,s must not be null */
    private Table join(String f, String s) {
        Table first = tableMap.get(f);
        Table second = tableMap.get(s);

        List<String> joinNames = new ArrayList();

        for (String c : first.getColNames()) {
            joinNames.add(c);
        }

        for (String c : second.getColNames()) {
            joinNames.add(c);
        }

        List<String> duplicates = new ArrayList<>();
        for (String c : first.getColNames()) {
            if (second.getColNames().contains(c)) {
                duplicates.add(c);
            }
        }

        for (int i = duplicates.size() - 1; i >= 0; i--) {
            while (joinNames.contains(duplicates.get(i))) {
                joinNames.remove(duplicates.get(i));
            }
            joinNames.add(0, duplicates.get(i));
        }

        Column[] cols = new Column[joinNames.size()];
        for (int i = 0; i < joinNames.size(); i++) {
            Column actualCol = findColumn(joinNames.get(i), new String[]{f, s});
            cols[i] = new Column(actualCol.getName(), actualCol.getColType());
        }
        Table temp = new Table(cols);

        for (int i = 0; i < first.size(); i++) {
            for (int j = 0; j < second.size(); j++) {
                List thirdRow = new ArrayList();

                for (String jName : joinNames) {
                    if (first.getColNames().contains(jName)) {
                        thirdRow.add(first.getColumn(jName).getItem(i).toString());
                    } else {
                        thirdRow.add(second.getColumn(jName).getItem(j).toString());
                    }
                }

                boolean addRow = true;

                for (String d : duplicates) {
                    if (!first.getColumn(d).getItem(i).equals(second.getColumn(d).getItem(j))) {
                        addRow = false;
                    }
                }


                if (addRow) {
                    temp.addRow(thirdRow);
                }
            }
        }

        return temp;
    }

    private Comparable parser(String item) {
        if (item.contains("\'")) {
            return item;
        } else if (item.contains(".")) {
            Float f = Float.parseFloat(item);
            return f;
        } else {
            Integer i = 0;
            try {
                i = Integer.parseInt(item);
            } catch (ClassCastException e) {
                return "ERROR: Wrong item type.";
            } catch (NumberFormatException e) {
                return item;
            }
            return i;
        }
    }

    private String noValueConv(String item, String type) {
        if (item.equals("NOVALUE")) {
            if (type.equals("string")) {
                return "''";
            } else if (type.equals("float")) {
                return "0.0";
            } else {
                return "0";
            }
        }
        return item;
    }

    private void filterHelper(Table preFilter, String[] condSplit) {
        for (int i = preFilter.size() - 1; i >= 0; i--) {
            if (isLiteral(condSplit[2])) {
                Comparable a = preFilter.getColumn(condSplit[0]).getItem(i);
                Comparable b = parser(condSplit[2]);
                if (!filter(a, b, condSplit[1])) {
                    preFilter.removeRow(i);
                }
            } else {
                if (!filter(preFilter.getColumn(condSplit[0]).getItem(i),
                        preFilter.getColumn(condSplit[2]).getItem(i), condSplit[1])) {
                    preFilter.removeRow(i);
                }
            }
        }
    }

    /* Performs a "select" operation with given expressions, tables, and conditions. */
    Table select(String expr, String[] tables, String cond) {
        Table preFilter = join(tables);
        createTable("$preFilter", preFilter);

        //Handles all case
        if (expr.equals("*")) {
            return preFilter;
        }

        //Handles both operator and non-operator cases
        String[] splitExpr;
        if (expr.contains(",")) {
            splitExpr = expr.split("\\s*,\\s*");
        } else {
            splitExpr = new String[]{expr};
        }

        Column[] allCols = new Column[splitExpr.length];
        try {
            for (int i = 0; i < splitExpr.length; i++) {
                if (!splitExpr[i].contains("+") && !splitExpr[i].contains("-")
                        && !splitExpr[i].contains("*") && !splitExpr[i].contains("/")) {
                    allCols[i] = findColumn(splitExpr[i], new String[]{"$preFilter"});
                } else {
                    allCols[i] = evaluateExpr(splitExpr[i]);
                }

            }

            //Reload all tables to prevent destructive behavior
            for (String t : tables) {
                load(t);
            }

            for (int i = 0; i < allCols.length; i++) {
                if (allCols[i] == null) {
                    return null;
                }
            }

            String[] multiCond = new String[]{};
            boolean condExists = false;
            if (!(cond == null)) {
                if (!cond.equals("")) {
                    multiCond = cond.split(" and ");
                    condExists = true;
                }
            }

            Table t = new Table(allCols);

            if (condExists) {
                for (String c : multiCond) {
                    String[] condSplit = c.split("\\s* \\s*");
                    filterHelper(t, condSplit);
                }

            }

            return t;

        } catch (ClassCastException e) {
            return null;
        }
    }

    private Column evaluateExpr(String expr) {
        if (!expr.contains(" as ")) {
            return null;
        }
        String[] colAndName = expr.split(" as ");
        colAndName[0] = colAndName[0].replaceAll("\\s* \\s*", "");
        String[] exprSplitChars = colAndName[0].split("");
        String[] exprSplit = new String[]{"", "", ""};
        boolean foundOperator = false;
        for (int i = 0; i < exprSplitChars.length; i++) {
            if (exprSplitChars[i].charAt(0) > 47 && exprSplitChars[i].charAt(0) != 46
                    && !foundOperator) {
                exprSplit[0] += exprSplitChars[i];
            } else if (exprSplitChars[i].charAt(0) <= 47 && exprSplitChars[i].charAt(0)
                    != 46 && !foundOperator) {
                exprSplit[1] = exprSplitChars[i];
                foundOperator = true;
            } else {
                exprSplit[2] += exprSplitChars[i];
            }
        }

        if (exprSplit[2].equals("")) {
            return null;
        }

        boolean literal = isLiteral(exprSplit[2]);

        if (!literal) {
            Column[] operands = new Column[]{findColumn(exprSplit[0], new String[]{"$preFilter"}),
                    findColumn(exprSplit[2], new String[]{"$preFilter"})};
            String operator = exprSplit[1];

            String colType = (operands[0].getColType().length() > operands[1].getColType().length())
                    ? operands[0].getColType() : operands[1].getColType();
            Column col = new Column(colAndName[1], colType);
            col = operate(operands, operator, colType, col);
            if (col == null) {
                return null;
            }

            return col;

        } else {
            return literalOperate(exprSplit, colAndName);
        }
    }

    private Column literalOperate(String[] exprSplit, String[] colAndName) {
        Column operand = findColumn(exprSplit[0], new String[]{"$preFilter"});
        String litString = "";
        float litFloat = 0.0f;
        if (exprSplit[2].charAt(0) == '\'') {
            litString = exprSplit[2];
        } else {
            litFloat = Float.parseFloat(exprSplit[2]);
        }

        String operator = exprSplit[1];

        String colType = operand.getColType();
        Column col = new Column(colAndName[1], colType);
        ArrayList colAdd = new ArrayList();
        if (operator.equals("+") && colType.equals("string") && !litString.equals("")) {
            for (int i = 0; i < operand.size(); i++) {
                String op0 = ((noValueConv(operand.getItem(i).toString(), "string")));
                op0 = (op0.length() > 0) ? op0.substring(0, op0.length() - 1) : op0;
                litString = litString.substring(1, litString.length());
                colAdd.add(op0 + litString);
            }
        } else if (litString.equals("")) {

            if (operator.equals("+")) {
                for (int i = 0; i < operand.size(); i++) {
                    colAdd.add((Float.parseFloat(noValueConv(operand.getItem(i).toString(),
                            "float")) + litFloat));
                }
            } else if (operator.equals("-")) {
                for (int i = 0; i < operand.size(); i++) {
                    colAdd.add((Float.parseFloat(noValueConv(operand.getItem(i).toString(),
                            "float")) - litFloat));
                }
            } else if (operator.equals("*")) {
                for (int i = 0; i < operand.size(); i++) {
                    colAdd.add((Float.parseFloat(noValueConv(operand.getItem(i).toString(),
                            "float")) * litFloat));
                }
            } else if (operator.equals("/")) {
                for (int i = 0; i < operand.size(); i++) {
                    colAdd.add((Float.parseFloat(noValueConv(operand.getItem(i).toString(),
                            "float")) / litFloat));
                }
            }

            if (colType.equals("int") && !exprSplit[2].contains(".")) {
                for (int i = 0; i < colAdd.size(); i++) {
                    col.addLast(String.valueOf((int) Math.floor((float) colAdd.get(i))));
                }
            } else if (colType.equals("float")) {
                for (int i = 0; i < colAdd.size(); i++) {
                    col.addLast(String.valueOf((float) colAdd.get(i)));
                }
            } else {
                for (int i = 0; i < colAdd.size(); i++) {
                    col.addLast(String.valueOf(colAdd.get(i)));
                }
            }
            return col;
        }
        return null;
    }

    private Column operate(Column[] operands, String operator, String colType, Column col) {
        try {
            if (operator.equals("+") && colType.equals("string")) {
                for (int i = 0; i < operands[0].size(); i++) {
                    String op0 = (String) operands[0].getItem(i);
                    op0 = op0.substring(0, op0.length() - 1);
                    String op1;
                    if (operands[1].getItem(i).equals("NOVALUE")) {
                        op1 = "''";
                    } else {
                        op1 = (String) operands[1].getItem(i);
                    }
                    op1 = op1.substring(1, op1.length());
                    col.addLast(op0 + op1);
                }
            } else if (colType.equals("float")) {
                for (int i = 0; i < operands[0].size(); i++) {
                    float op1;
                    float op0 = Float.parseFloat(String.valueOf(operands[0].getItem(i)));
                    if (operands[1].getItem(i).equals("NOVALUE")) {
                        op1 = 0.0f;
                    } else {
                        op1 = Float.parseFloat(String.valueOf(operands[1].getItem(i)));
                    }
                    if (operator.equals("+")) {
                        col.addLast(String.valueOf(op0 + op1));
                    } else if (operator.equals("-")) {
                        col.addLast(String.valueOf(op0 - op1));
                    } else if (operator.equals("*")) {
                        col.addLast(String.valueOf(op0 * op1));
                    } else if (operator.equals("/")) {
                        if (op1 == 0.0) {
                            col.addLast("NaN");
                        } else {
                            col.addLast(String.valueOf(op0 / op1));
                        }
                    }
                }
            } else {
                for (int i = 0; i < operands[0].size(); i++) {
                    int op1;
                    if (operands[1].getItem(i).equals("NOVALUE")) {
                        op1 = 0;
                    } else {
                        op1 = (int) operands[1].getItem(i);
                    }
                    if (operator.equals("+")) {
                        col.addLast(String.valueOf((int) operands[0].getItem(i)
                                + op1));
                    } else if (operator.equals("-")) {
                        col.addLast(String.valueOf((int) operands[0].getItem(i)
                                - op1));
                    } else if (operator.equals("*")) {
                        col.addLast(String.valueOf((int) operands[0].getItem(i)
                                * op1));
                    } else if (operator.equals("/")) {
                        col.addLast(String.valueOf((int) operands[0].getItem(i)
                                / op1));
                    }
                }
            }
            return col;
        } catch (ClassCastException e) {
            return null;
        }
    }

    private boolean isLiteral(String s) {
        return (s.charAt(0) == '\'' || s.charAt(0) < 58);
    }

    private boolean filter(Comparable a, Comparable b, String compare) {
        if (a.equals("NOVALUE") || b.equals("NOVALUE")) {
            return false;
        }
        if (a.equals("NaN")) {
            a = Float.MAX_VALUE;
        }
        if (b.equals("NaN")) {
            b = Float.MAX_VALUE;
        }
        if (compare.equals("<")) {
            return a.compareTo(b) < 0;
        } else if (compare.equals(">")) {
            return a.compareTo(b) > 0;
        } else if (compare.equals("<=")) {
            return a.compareTo(b) <= 0;
        } else if (compare.equals(">=")) {
            return a.compareTo(b) >= 0;
        } else if (compare.equals("==")) {
            return a.equals(b);
        } else {
            return !a.equals(b);
        }
    }

    private Column findColumn(String name, String[] t) {
        for (String table : t) {
            for (String c : tableMap.get(table).getColNames()) {
                if (name.equalsIgnoreCase(c)) {
                    return tableMap.get(table).getColumn(c);
                }
            }
        }
        return null;
    }

    /* Prints a table with given name */
    String print(String name) {
        if (!tableMap.containsKey(name)) {
            return ("ERROR: Table does not exist.");
        } else {
            return (tableMap.get(name).toString());
        }
    }

    private String addRow(String name, List row) {
        if (tableMap.containsKey(name)) {
            return tableMap.get(name).addRow(row);
        } else {
            return "ERROR: Table does not exist.";
        }
    }

    public static void main(String[] args) {
        Database db = new Database();
        db.transact("load examples/t1");
        System.out.println(db.transact("load examples/records"));
        System.out.println(db.transact("load examples/teams"));
        System.out.println(db.transact("load examples/fans"));
        System.out.println(db.transact("load examples/seasonRatios"));
        System.out.println(db.transact("insert into examples/seasonRatios values 'Los Altos',2017,3"));
        System.out.println(db.transact("print examples/seasonRatios"));
        System.out.println(db.transact("store examples/seasonRatios"));
        System.out.println(db.transact("drop table examples/seasonRatios"));




//        System.out.println(db.transact("print examples/fans"));
//        System.out.println(db.transact("create table examples/seasonRatios as select City,Season,Wins/Losses as Ratio from examples/teams,examples/records"));
//        System.out.println(db.transact("create table examples/seasonRatios as select City,Season,Wins/Losses as Ratio from examples/teams,examples/records"));
//
//        System.out.println(db.transact("print examples/seasonRatios"));
//        System.out.println(db.transact("store examples/seasonRatios"));
//        System.out.println(db.transact("select Firstname,Lastname,TeamName from examples/fans where Lastname >= 'Lee'"));
//        System.out.println(db.transact("select Mascot,YearEstablished from examples/teams where YearEstablished > 1942"));
//    }
    }
}

