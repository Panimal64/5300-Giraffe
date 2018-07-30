/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
#include <algorithm>
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
		    case ColumnAttribute::BOOLEAN:
			out << (value.n == 0 ? "false" : "true");
			break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
  if(column_names)
    delete column_names;
  if(column_attributes)
    delete column_attributes;
  if(rows)
  {
    for (ValueDict *row: *rows)
      delete row;
    delete rows;
  }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // FIXME: initialize _tables table, if not yet present
    if(!SQLExec::tables)
      SQLExec::tables = new Tables();
    if(!SQLExec::indices)
      SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError("Statement not yet handled");
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
  column_name = col->name;
  switch (col->type) {
    case ColumnDefinition::INT:
      column_attribute.set_data_type(ColumnAttribute::INT);
      break;
    case ColumnDefinition::TEXT:
      column_attribute.set_data_type(ColumnAttribute::TEXT);
      break;
    default:
      throw SQLExecError("Can only handle TEXT or INT");
  }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
	case CreateStatement::kTable:
	    return create_table(statement);
	case CreateStatement::kIndex:
	    return create_index(statement);
	default:
	    return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}


// Code mainly translated from Python
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
  if(statement->type != CreateStatement::kTable)
    return new QueryResult("Only handling CREATE TABLE at the moment");

  // update _tables schema
  Identifier name = statement->tableName;
  ValueDict row;
  row["table_name"] = name;


  ColumnNames column_order;
  ColumnAttributes column_attributes;

  Identifier columnName;
  ColumnAttribute attribute;
  for(ColumnDefinition *column : *statement->columns)
  {
    column_definition(column, columnName, attribute);
    column_order.push_back(columnName);
    column_attributes.push_back(attribute);
  }

  Handle tableHandle = SQLExec::tables->insert(&row);

  try
  {
    // update _columns schema
    Handles columnHandles;
    DbRelation& _columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    try{
      int count = 0;
      for(auto const& column_name : column_order)
      {
        row["column_name"] = column_name;

        switch(column_attributes[count].get_data_type())
        {
          case ColumnAttribute::INT:
            row["data_type"] = Value("INT");
            break;
          case ColumnAttribute::TEXT:
            row["data_type"] = Value("TEXT");
            break;
          default:
            throw SQLExecError("Can only handle TEXT or INT");
        }

       columnHandles.push_back(_columns.insert(&row));
       count++;
     }

     // Create table
     DbRelation& _tables = SQLExec::tables->get_table(name);
     if(statement->ifNotExists)
      _tables.create_if_not_exists();
     else
      _tables.create();
   }
   catch(exception &e)
   {
     // attempt to undo the insertions into _columns
     try{

        for(auto const &h : columnHandles)
        {
          _columns.del(h);
        }
      }
      catch(exception &e)
      {

      }
      throw "Unable to insert into _columns";
   }
  }
  catch(exception& e)
  {
    // attempt to undo the insertion into _tables
    try{
      SQLExec::tables->del(tableHandle);
    }
    catch(exception& e)
    {

    }
    throw "Unable to insert into _tables";
  }

  return new QueryResult("Created " + name);
}


QueryResult *SQLExec::create_index(const CreateStatement *statement){
  // Get the underlying table.
  Identifier tableName = statement->tableName;
  // DbRelation& _tables = SQLExec::tables->get_table(tableName);

  // Check that all the index columns exist in the table.
  ColumnNames column_order;
  ColumnAttributes column_attributes;

  ColumnAttribute attribute;

  ColumnNames table_column_order;
  ColumnAttributes table_column_attributes;

  SQLExec::tables->get_columns(tableName, table_column_order, table_column_attributes);

  bool found;
  int count;

  for(Identifier columnName: *statement->indexColumns)
  {
    found = false;
    count = 0;

    for(auto const& tableColumn : table_column_order)
    {
      if(columnName == tableColumn)
      {
        found = true;
        column_order.push_back(columnName);
        column_attributes.push_back(table_column_attributes[count]);
        count++;
      }
    }
    if(!found)
    {
      std::cout << "Column not found" << endl;
      throw "Column not found";
    }
  }

  Identifier indexName = statement->indexName;
  ValueDict row;
  int seq_in_index = 0;

  row["index_name"] = indexName;
  row["table_name"] = tableName;
  row["seq_in_index"] = seq_in_index++;

  Identifier indexType = statement->indexType;
  row["index_type"]  = indexType;

  if(indexType == "BTREE")
    row["is_unique"] = true;
  else if (indexType == "HASH")
    row["is_unique"] = false;
  else
  {
    indexType = "BTREE";
    row["index_type"]  = indexType;
    row["is_unique"] = true;
  }

  // Insert a row for each column in index key into _indices.
  // I recommend having a static reference to _indices in SQLExec, as we do for _tables.

      count = 0;
      for(auto const& column_name : column_order)
      {
        row["column_name"] = column_name;
        row["seq_in_index"] = seq_in_index++;

        switch(column_attributes[count].get_data_type())
        {
          case ColumnAttribute::INT:
            row["data_type"] = Value("INT");
            break;
          case ColumnAttribute::TEXT:
            row["data_type"] = Value("TEXT");
            break;
          case ColumnAttribute::BOOLEAN:
            row["data_type"] = Value("BOOLEAN");
            break;
          default:
            throw SQLExecError("Can only handle TEXT, INT, or BOOLEAN");
        }

        SQLExec::indices->insert(&row);
        count++;
     }

     // Call get_index to get a reference to the new index and then invoke the create method on it.
     DbIndex& index = SQLExec::indices->get_index(tableName, indexName);

     index.create();


    return new QueryResult("Created " + indexName);
}


QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
	case DropStatement::kTable:
	    return drop_table(statement);
	case DropStatement::kIndex:
	    return drop_index(statement);
	default:
	    return new QueryResult("Only DROP TABLE and DROP INDEX are implemented");
    }
}

// Translated from python
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
	if (statement->type != DropStatement::kTable){
	    throw SQLExecError("Unrecognized drop type!");
	} //from prompt

	Identifier name = statement->name;
	if (name == Tables::TABLE_NAME || name == Columns::TABLE_NAME || name == Indices::TABLE_NAME){
	    throw SQLExecError("Cannot drop the schema!");
	}

	ValueDict select_name;
	select_name["table_name"] = Value(name);

	DbRelation& table = SQLExec::tables->get_table(name);
	DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

	//get indices for deletion
	IndexNames all_indices = SQLExec::indices->get_index_names(name);
	for (auto const& index: all_indices){
	    DbIndex& to_drop = SQLExec::indices->get_index(name, index);
	    Handles* temp_handles = SQLExec::indices->select(&select_name); //this might be weird
	    to_drop.drop();
	    for (auto const& handle: *temp_handles){
		SQLExec::indices->del(handle);
	    }
	    delete temp_handles;
	}

	Handles* handles = columns.select(&select_name);

	for(auto const& row: *handles){
	    columns.del(row);
	}
	delete handles;

	table.drop(); //done in order per prompt
	SQLExec::tables->del(*SQLExec::tables->select(&select_name)->begin());
	return new QueryResult(string("Dropped ") + name);
}

// translated from python
QueryResult *SQLExec::drop_index(const DropStatement *statement){
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->name;

    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    ValueDict select_name;
    select_name["table_name"] = Value(table_name);
    select_name["index_name"] = Value(index_name);

    Handles* handles = SQLExec::indices->select(&select_name);
    index.drop();
    for (auto const& handle: *handles){
	SQLExec::indices->del(handle);
    }

    delete handles;

    return new QueryResult("Dropped index: " + index_name);
}


QueryResult *SQLExec::show(const ShowStatement *statement) {
	switch(statement->type)
  {
    case ShowStatement::kTables:
      return show_tables();
    case ShowStatement::kColumns:
      return show_columns(statement);
    case ShowStatement::kIndex:
      return show_index(statement);
    default:
      throw SQLExecError("Can only show _tables or _columns");
  }
}

// FIX ME
QueryResult *SQLExec::show_index(const ShowStatement *statement){
    std::string message;
    int length = 0;
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ColumnAttributes* attributes = new ColumnAttributes;
    attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    attributes->push_back(ColumnAttribute(ColumnAttribute::INT));
    attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict select_name;
    select_name["table_name"] = Value(statement->tableName);

    Handles* handles = SQLExec::indices->select(&select_name);
    length = handles->size();
    ValueDicts* index_rows = new ValueDicts();
    for (auto const& handle: *handles){
	ValueDict* row = SQLExec::indices->project(handle, column_names);
	index_rows->push_back(row);
    }
    delete handles;

    message += "Successfully returned ";
    message += to_string(length);
    message += " rows";

    return new QueryResult(column_names, attributes, index_rows, message);
}

QueryResult *SQLExec::show_tables() {
  std::string message;
  int count = 0;

  ColumnNames* names = new ColumnNames;
  names->push_back("table_name");

  ColumnAttributes* attributes = new ColumnAttributes;
  attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

  Handles* handles = SQLExec::tables->select();

  ValueDicts* rows = new ValueDicts;
  for (auto const& handle: *handles) {
    ValueDict* row = SQLExec::tables->project(handle, names);

    if(row->at("table_name").s != "table_names")
    {
      rows->push_back(row);
      count++;
    }
  }

  message += "Successfully returned ";
  message += to_string(count);
  message += " rows\n";
  return new QueryResult(names, attributes, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
	std::string message;
	int length = 0;

	DbRelation& relation = SQLExec::tables->get_table(Columns::TABLE_NAME);

	ColumnNames* names = new ColumnNames;
	names->push_back("table_name");
	names->push_back("column_name");
	names->push_back("data_type");

	ColumnAttributes* attributes = new ColumnAttributes;
	attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	ValueDict select_name;
	select_name["table_name"] = Value(statement->tableName);
	Handles* handles = relation.select(&select_name);
	length = handles->size();

	ValueDicts* rows = new ValueDicts;
	for(auto const& index: *handles){
	    ValueDict* single_row = relation.project(index, names);
	    rows->push_back(single_row);
	}
	delete handles;

	message = "Successfully returned " + to_string(length) + " rows";
	return new QueryResult(names, attributes, rows, message); // FIXME
}
