# Custom In-Memory Database Management System (DBMS)

A lightweight, relational database management system built entirely from scratch using **Python**. This project simulates core SQL functionalities without relying on external libraries like `pandas`, `sqlite3`, or `SQLAlchemy`.

The primary goal of this project was to understand the underlying logic of **database engines**, **query parsing**, and **data structure manipulation** at a low level.

## 🚀 Key Features

* **No External Dependencies:** Built using only Python's standard library and native data structures (Dictionaries, Lists, Sets).
* **Custom Query Parser:** Implements a custom algorithm to parse and tokenize SQL-like commands and `WHERE` clauses.
* **CRUD Operations:** Full support for `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE`.
* **Relational Logic:** Supports `JOIN` operations to merge data from two tables based on a common column.
* **CLI Interface:** Processes commands directly from a text file via the command line interface.
  
## 💻 Usage
```
    You need to provide an input file containing the commands as an argument.
    ```bash
    python database.py input.txt
    ```

## 📝 Command Syntax (Input File Format)

The system accepts a `.txt` file where each line is a command. Below are the supported formats based on the custom parser:

### 1. Create Table
Creates a new table with specified columns.
```sql
CREATE_TABLE students id,name,age,major
```

### 2. Insert Data

Inserts a row into the specified table.

```sql
INSERT students 1,John Doe,20,CS
INSERT students 2,Jane Smith,22,EE
```

### 3. Select Data

Selects specific columns or all columns (`*`) with optional conditions.

```sql
SELECT students id,name WHERE {"major": "CS"}
SELECT students id,name,age WHERE {"major": "CS", "age": "21"}
```

### 4. Update Data

Updates records based on a condition.

```sql
UPDATE students {"major": "SE"} WHERE {"name": "John Doe"}
```

### 5. Join Tables

Joins two tables based on a common column.

```sql
JOIN students,courses ON major
```

### 6. Delete Data

Deletes specific rows or truncates the table.

```sql
DELETE students WHERE {"age": 22}
```

### 7. Count Rows

Counts the number of rows matching a condition.

```sql
COUNT students WHERE {"major": "CS"}
```

