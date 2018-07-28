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
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
	throw SQLExecError("not implemented");  // FIXME
}

//Charlie's used python as guideline
QueryResult *SQLExec::create(const CreateStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
	if (statement->type != DropStatement::kTable){
	    throw SQLExecError("Unrecognized drop type!");
	} //from prompt

	Identifier name = statement->name;
	if (name == Tables::TABLE_NAME || name == Columns::TABLE_NAME){
	    throw SQLExecError("Cannot drop the schema!");
	}

	ValueDict select_name;
	select_name["table_name"] = Value(name);

	DbRelation& table = SQLExec::tables->get_table(name);
	DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
	Handles* handles = columns.select(&select_name);

	for(auto const& index: *handles){
	    columns.del(index);
	}
	delete handles;

	table.drop(); //done in order per prompt
	SQLExec::tables->del(*SQLExec::tables->select(&select_name)->begin());
	return new QueryResult(string("Dropped ") + name);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
	return new QueryResult("not implemented"); // FIXME
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

