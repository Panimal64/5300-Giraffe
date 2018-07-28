/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

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
	// FIXME
}


QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // FIXME: initialize _tables table, if not yet present
    if(!SQLExec::tables)
    {
      SQLExec::tables = new Tables();

    }

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

// Code mainly translated from Python
QueryResult *SQLExec::create(const CreateStatement *statement) {
  // update _tables schema
  Identifier name = statement->tableName;
  ValueDict row;
  row["table_name"] = name;
  Handle tableHandle = SQLExec::tables->insert(&row);

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
          case ColumnAttribute::TEXT:
            row["data_type"] = Value("TEXT");
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
      tables->del(tableHandle);
    }
    catch(exception& e)
    {

    }
    throw "Unable to insert into _tables";
  }

  return new QueryResult("created" + name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
  std::string message;
  int count = 0;

  ColumnNames* names = new ColumnNames;
  names->push_back("table_names");

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
  message += count;
  message += " rows\n";
  return new QueryResult(names, attributes, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}
