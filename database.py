import sys


def create_table(database, command):
    # Function to append the list inside a database dictionary, and store the columns in another dictionary inside it.
    print("###################### CREATE #########################")
    try:
        columns = (command[2].strip()).split(",")
        if command[1] in database:
            raise ValueError
        elif columns == ['']:
            raise IndexError
        database[command[1]] = {}
        for column in columns:
            database[command[1]].update({column : []})
        print("Table '{}' created with columns: {}".format(command[1], columns))
    except ValueError:
        print("Table '{}' already exists.".format(command[1]))
    except IndexError:
        print("Couldn't create table: no columns given.")
    print("#######################################################\n")


def insert_table(database, command):
    print("###################### INSERT #########################")
    try:
        data = command[2].split(",")
        table_name = command[1]

        if table_name not in database:
            raise ValueError("Table '{}' does not exist.".format(table_name))

        if len(data) != len(database[table_name]):
            raise IndexError

        # Inserting the given elements in columns respectively.
        for key,value in (list(zip(database[table_name].keys(), data))):
            database[table_name][key].append(value)

        print("Inserted into '{}': {}\n".format(table_name, tuple(data)))
        print_table(database, table_name)

    except IndexError:
        print("Given data does not match the number of columns.")
    except ValueError as error:
        print(error)
    finally:
        print("#######################################################\n")


def select_table(database, command):
    print("###################### SELECT #########################")
    try:
        args = command[2].split(" WHERE ")
        table_name = command[1]

        if table_name not in database:
            raise KeyError("Table '{}' does not exist.".format(table_name))

        database_columns = list(database[table_name].keys())
        conditions = args[1]
        indexes = condition_analyzer(database, conditions, table_name)

        selected_columns = args[0].split(",")

        if args[0] == "*":
            selected_columns = database_columns.copy()
        rows = []
        rows_results = []

        # Checking for column existence
        for column in selected_columns:
            if column not in database[table_name]:
                raise KeyError("Column {} does not exist.".format(column))

        # Using the indexes from given conditions, acquiring the values of the indexes from the selected columns.
        for index in indexes:
            for column in selected_columns:
                rows.append(database[table_name][column][index])
            rows_results.append(tuple(rows))
            rows.clear()

        print("Condition: {}".format(conditions))
        print("Select result from '{}': {}".format(table_name, rows_results))
    except KeyError as error:
        print(str(error))
        return
    finally:
        print("#######################################################\n")


def update_table(database, command):
    print("###################### UPDATE #########################")
    try:
        args = command[2].split(" WHERE ")
        updates = args[0]
        table_name = command[1]
        if table_name not in database:
            raise KeyError("Table '{}' does not exist.".format(table_name))
        conditions = args[1]

        updates_processed = updates.strip("{}").split(",")
        updating_columns, updating_cleaned_columns, updating_elements, updating_cleaned_elements = [], [], [], []

        # Below part works exactly like condition_analyzer, but does not return the indexes, only the columns and rows.
        for condition in updates_processed:
            condition = condition.split(":")
            updating_columns.append(condition[0])
            updating_elements.append(condition[1])

        for column in updating_columns:
            column = column.strip(' "')
            column = column.strip(" '")
            updating_cleaned_columns.append(column)

        for column in updating_cleaned_columns:
            if column not in database[table_name]:
                raise KeyError("Column {} does not exist".format(column))

        for row in updating_elements:
            row = row.strip(' "')
            row = row.strip(" '")
            updating_cleaned_elements.append(row)

        # Using condition_analyzer to find indexes, and replacing the given columns with given values.
        indexes_to_update = condition_analyzer(database, conditions, table_name)
        for index in indexes_to_update:
            for key,value in zip(updating_cleaned_columns, updating_cleaned_elements):
                database[table_name][key][index] = value

        print("Updated '{}' with {} where {}".format(table_name, updates, conditions))
        print("{} rows updated.\n".format(len(indexes_to_update)))
        print_table(database, table_name)
    except KeyError as error:
        print(str(error))
        return
    finally:
        print("#######################################################\n")


def delete_rows(database, command):
    print("###################### DELETE #########################")
    try:
        if len(command) == 2:
            table_name = command[1]
            if table_name not in database:
                raise KeyError("Table '{}' does not exist.".format(table_name))
            any_column = list(database[table_name].keys())[0]
            deleted_row_count = len(database[table_name][any_column])
            for column in database[table_name]:
                database[table_name][column].clear()
            print("Deleted all rows from '{}'".format(table_name))

        elif len(command) == 3:
            table_name = command[1]
            if table_name not in database:
                raise KeyError("Table {} does not exist".format(table_name))
            conditions = command[2].strip(" WHERE ")
            # Sorting the indexes in descending order so when it deletes for example the 2nd index, it wouldn't cause a
            # problem, for example making the bigger indexes decrease by 1, then deleting the wrong element.
            indexes = sorted(condition_analyzer(database, conditions, table_name), reverse = True)
            deleted_row_count = len(indexes)
            for index in indexes:
                for column in database[table_name]:
                    del database[table_name][column][index]
            print("Deleted from '{}' where {}".format(table_name, conditions))

        print("{} rows deleted.\n".format(deleted_row_count))
        print_table(database, table_name)

    except KeyError as error:
        print(str(error))
        return
    finally:
        print("#######################################################\n")


def join_tables(database, command):
    try:
        table_name1, table_name2 = command[1].split(",")
        common_column = command[2].strip(" ON ")
        print("####################### JOIN ##########################")
        print("Join tables {} and {}".format(table_name1, table_name2))

        if table_name1 not in database:
            raise KeyError("Table {} does not exist".format(table_name1))
        elif table_name2 not in database:
            raise KeyError("Table {} does not exist".format(table_name2))
        if common_column not in database[table_name1] or common_column not in database[table_name2]:
            raise KeyError("Column {} does not exist".format(common_column))

        all_columns, rows_database, burner_list, maxlengths, lengths = [], [], [], [], []

        for column in database[table_name1]:
            all_columns.append(column)
        for column in database[table_name2]:
            all_columns.append(column)

        # Checking for matching values for given column, and if it matches, appends the joined row to rows_database.
        for index, value in enumerate(database[table_name1][common_column]):
            for i, val in enumerate(database[table_name2][common_column]):
                if value == val:
                    for column in database[table_name1]:
                        burner_list.append(database[table_name1][column][index])
                    for column in database[table_name2]:
                        burner_list.append(database[table_name2][column][i])
                    rows_database.append(list(burner_list))
                    burner_list.clear()

        # Had to write the print section separately as my print_table function works on dictionaries and dictionaries
        # can't store duplicate keys. But it works nearly with the same logic.
        for index in range(len(all_columns)):
            for row in rows_database:
                 lengths.append(len(row[index]))
            max_len = max(len(all_columns[index]), max(lengths))
            maxlengths.append(max_len)
            lengths.clear()

        print("Join result: ({} rows):\n".format(len(rows_database)))


        horizontal_divider = "+"
        for i in maxlengths:
            horizontal_divider = horizontal_divider + ("-" * i) + "--+"

        column_divider = "|"
        for element, length in zip(all_columns, maxlengths):
            column_divider = column_divider + " " + str(element) + " " * (length - len(str(element))) + " |"

        print("Table: Joined Table")
        print(horizontal_divider)
        print(column_divider)
        print(horizontal_divider)

        for row in rows_database:
            for i in range(len(row)):
                t = 0
                value_divider = "|"
                for value in row:
                    value_divider = value_divider + " " + str(value) + " " * (maxlengths[t] - len(str(value))) + " |"
                    t = t + 1
            print(value_divider)

        print(horizontal_divider)

    except KeyError as error:
        print(str(error))
    finally:
        print("#######################################################\n")


def count_table(database, command):
    print("###################### COUNT #########################")
    try:
        # Just uses the function condition_analyzer to find corresponding indexes, and printing the length of the list of indexes.
        conditions = command[2].strip(" WHERE ")
        table_name = command[1]
        if table_name not in database:
            raise KeyError("Table {} does not exist".format(table_name))
        any_column = list(database[table_name].keys())[0]
        if conditions == "*":
            print("Count: {}".format(len(database[table_name][any_column])))
            print("Total number of entries in '{}' is {}".format(table_name, len(database[table_name][any_column])))
        else:
            indexes = condition_analyzer(database, conditions, table_name)
            print("Count: {}".format(len(indexes)))
            print("Total number of entries in '{}' is {}".format(table_name, len(indexes)))

    except KeyError as error:
        print(str(error))
    finally:
        print("#######################################################\n")


def condition_analyzer(database, conditions, table_name):
    # Returns the indexes of the values satisfying the given conditions from the given database table.
        conditions = conditions.strip("{}").split(",")
        columns, cleaned_columns, rows, cleaned_rows, indexes, index_sets = [], [], [], [], [], []

        # Purifying the condition (getting rid of punctuations)
        for condition in conditions:
            condition = condition.split(":")
            columns.append(condition[0])
            rows.append(condition[1])
        for column in columns:
            column = column.strip(' "')
            column = column.strip(" '")
            cleaned_columns.append(column)

        for column in cleaned_columns:
            if column not in database[table_name]:
                raise KeyError("Column {} does not exist".format(column))

        for row in rows:
            row = row.strip(' "')
            row = row.strip(" '")
            cleaned_rows.append(row)

        # Checking for indexes for the condition
        for key, value in (list(zip(cleaned_columns, cleaned_rows))):
            for i in range(len(database[table_name][key])):
                if database[table_name][key][i] == value:
                 indexes.append(i)
            index_sets.append(set(indexes))
            indexes.clear()

        # Taking the common indexes of different conditions.
        common_indexes = set.intersection(*index_sets)
        return common_indexes


def maxlength(list_to_evaluate):
    # Returns the length of the longest element in the given list. I don't know if this was necessary, but I couldn't
    # come up with another solution.
    if not list_to_evaluate:
        return 0
    lengths = []
    for i in list_to_evaluate:
        lengths.append(len(str(i)))
    return max(lengths)


def print_table(database, table_name):
    # General function to print tables.

    maxlengths = []
    column_names = []

# Getting the maximum lengths for each column and row, and adapting the table's width according to the values.
    # Printing the header, which contains column names.
    for column in database[table_name]:
        maxlengths.append((max(len(column), maxlength(database[table_name][column]))))
        column_names.append(column)

    horizontal_divider = "+"
    for i in maxlengths:
        horizontal_divider = horizontal_divider + ("-" * i) + "--+"

    column_divider = "|"
    for element, length in zip(column_names, maxlengths):
        column_divider = column_divider + " " + str(element) + " " * (length - len(str(element))) + " |"

    print("Table: {}".format(table_name))
    print(horizontal_divider)
    print(column_divider)
    print(horizontal_divider)

    # Printing all the rows.
    values = list(zip(*database[table_name].values()))
    for i in range(len(values)):
        t=0
        value_divider = "|"
        for a in values[i]:
            value_divider = value_divider + " " + str(a) + " " * (maxlengths[t] - len(str(a))) + " |"
            t = t+1
        print(value_divider)

    print(horizontal_divider)


def main():
    # Gets the commands from the given text file, and uses the corresponding function according to the first word.
    try:
        input_file = "".join(open(sys.argv[1], 'r').readlines()).split("\n")
        tables_database = {}
    except IndexError:
        print("No input file given.")
        return
    except FileNotFoundError:
        print("Specified file does not exist.")
        return

    for i in input_file:
        if i == "":
            input_file.remove(i)
    for line in input_file:
        try:
            line = line.split(None, 2) # Splitting by 2 so the spaces in names do not get split as well.

            if line[0] == "CREATE_TABLE":
                  create_table(tables_database,line)
            elif line[0] == "INSERT":
                insert_table(tables_database,line)
            elif line[0] == "SELECT":
                 select_table(tables_database,line)
            elif line[0] == "UPDATE":
                  update_table(tables_database,line)
            elif line[0] == "DELETE":
                    delete_rows(tables_database,line)
            elif line[0] == "JOIN":
                  join_tables(tables_database,line)
            elif line[0] == "COUNT":
                count_table(tables_database,line)
            else:
                raise ValueError("Invalid command: {}".format(line[0]))
        except ValueError as error:
            print(str(error))


if __name__ == '__main__':
    main()