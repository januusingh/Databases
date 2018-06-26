package db;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;

public class dbParse {
    // Various common constructs, simplifies parsing.
    private static final String REST = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD = Pattern.compile("load " + REST),
            STORE_CMD = Pattern.compile("store " + REST),
            DROP_CMD = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW = Pattern.compile("(\\S+)\\s+\\((\\S+\\s+\\S+\\s*" +
            "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+" +
                    "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+" +
                    "([\\w\\s+\\-*/'<>=!]+?(?:\\s+and\\s+" +
                    "[\\w\\s+\\-*/'<>=!]+?)*))?"),
            CREATE_SEL = Pattern.compile("(\\S+)\\s+as select\\s+" +
                    SELECT_CLS.pattern()),
            INSERT_CLS = Pattern.compile("(\\S+)\\s+values\\s+(.+?" +
                    "\\s*(?:,\\s*.+?\\s*)*)");

    public static String parse(Database d, String[] args) {
        if (args.length != 1) {
            return "ERROR: Expected a single query argument";
        } else {
            return eval(d, args[0]);
        }
    }

    private static String eval(Database d, String query) {
        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(d, m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(d, m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return storeTable(d, m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return dropTable(d, m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insertRow(d, m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(d, m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(d, m.group(1));
        } else {
            return ("ERROR: Malformed query: " + query + "\n");
        }
    }

    private static String createTable(Database d, String expr) {
        Matcher m;
        String result;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            int len = (m.group(2).split(COMMA)).length;
            String[] names = new String[len];
            String[] types = new String[len];
            for (int i = 0; i < len; i++) {
                String[] temp = m.group(2).split(COMMA)[i].split("\\s* \\s*");
                names[i] = temp[0];
                types[i] = temp[1];
            }
            result = createNewTable(m.group(1), names, types, d);
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            result = createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4), d);
        } else {
            result = "ERROR: Malformed create: " + expr;
        }
        return result;
    }

    private static String createNewTable(String name, String[] cols, String[] types, Database d) {
        Column[] c = new Column[cols.length];
        for (int i = 0; i < cols.length; i++) {
            if (!types[i].equals("notype")) {
                c[i] = new Column(cols[i], types[i]);
            }
            else {
                return "ERROR: Type: NOTYPE";
            }
        }
        d.createTable(name, c);
        return "";
    }

    private static String createSelectedTable(String name, String exprs, String tables, String conds, Database d) {
        Table selected = d.select(exprs, tables.split(","), conds);
        if (selected != null) {
            d.createTable(name, selected);
            return "";
        }
        return "ERROR: Bad format/typing";
    }

    private static String loadTable(Database d, String name) {
        return (d.load(name));
    }

    private static String storeTable(Database d, String name) {
        return d.store(name);
    }

    private static String dropTable(Database d, String name) {
        return d.drop(name);
    }

    private static String insertRow(Database d, String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed insert: " + expr + "\n";
        }
        int len = (m.group(0).split(COMMA)).length;
        ArrayList<String> values = new ArrayList();
        for (int i = 0; i < len; i++) {
            values.add(m.group(2).split(COMMA)[i]);
        }
        return d.insert(m.group(1), values);
    }

    private static String printTable(Database d, String name) {
        return String.valueOf(d.print(name));
    }

    private static String select(Database d, String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return ("ERROR: Malformed select: " + expr);

        }
        String[] condSplit = expr.split(" from ");
        String[] secondSplit;
        if (condSplit[1].contains(" where ")) {
            secondSplit = condSplit[1].split(" where ");
        }
        else {
            secondSplit = new String[] {condSplit[1], ""};
        }
        return select(d, condSplit[0], secondSplit[0], secondSplit[1]);
    }

    private static String select(Database d, String exprs, String tables, String conds) {
        Table selected = d.select(exprs, tables.split(","), conds);
        if (selected != null) {
            return selected.toString();
        }
        return "ERROR: Malformed select statement";
    }
}