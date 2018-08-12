/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen, Minh Nguyen, Dave Pannu
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
#include "EvalPlan.h"
#include <algorithm>
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
	if (qres.column_names != nullptr) {
		for (auto const &column_name : *qres.column_names)
			out << column_name << " ";
		out << endl << "+";
		for (unsigned int i = 0; i < qres.column_names->size(); i++)
			out << "----------+";
		out << endl;
		for (auto const &row : *qres.rows) {
			for (auto const &column_name : *qres.column_names) {
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

//delete the remaining of dangling pointers
QueryResult::~QueryResult() {
	if (column_names)
		delete column_names;
	if (column_attributes)
		delete column_attributes;
	if (rows)
	{
		for (ValueDict *row : *rows)
			delete row;
		delete rows;
	}
}

//acts as a triage to call an appropriate method to handle a SQL statement
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
	// FIXME: initialize _tables table, if not yet present
	if (!SQLExec::tables)
		SQLExec::tables = new Tables();
	if (!SQLExec::indices)
		SQLExec::indices = new Indices();

	try {
		switch (statement->type()) {
		case kStmtCreate:
			return create((const CreateStatement *)statement);
		case kStmtDrop:
			return drop((const DropStatement *)statement);
		case kStmtShow:
			return show((const ShowStatement *)statement);
		case kStmtInsert:
			return insert((const InsertStatement *)statement);
		case kStmtDelete:
			return del((const DeleteStatement *)statement);
		case kStmtSelect:
			return select((const SelectStatement *)statement);
		default:
			return new QueryResult("not implemented");
		}
	}
	catch (DbRelationError& e) {
		throw SQLExecError(string("DbRelationError: ") + e.what());
	}
}

//Pull out conjunctions of equality predicates from parse tree. 
ValueDict* SQLExec::get_where_conjunction(const hsql::Expr *expr, const ColumnNames *col_names) {
	
	ValueDict* rows = new ValueDict;
	//Check if where clause has an operator
	if (expr->type == kExprOperator) {
		if (expr->opType == Expr::AND) {
			//recursively get left expr
			ValueDict* sub_tree_conj = get_where_conjunction(expr->expr, col_names);
			if (sub_tree_conj != nullptr) {
				rows->insert(sub_tree_conj->begin(), sub_tree_conj->end());
			}
			//recursively get right expr
			sub_tree_conj = get_where_conjunction(expr->expr2, col_names);
			rows->insert(sub_tree_conj->begin(), sub_tree_conj->end());

		}
		else if (expr->opType == Expr::SIMPLE_OP) {
			//Check and handle equality predicate
			if (expr->opChar == '=') {
				//Extract column and check if exists in table columns
				Identifier col_name = expr->expr->name;
				if (find(col_names->begin(), col_names->end(), col_name) == col_names->end()) {
					throw SQLExecError("unknown column '" + col_name + "'");
				}

				//Check if the value type is valid and add to return ValueDict
					switch (expr->expr2->type) {
					case kExprLiteralString: 
					{
						rows->insert(pair<Identifier, Value>(col_name, Value(expr->expr2->name)));
						break;
					}
					case kExprLiteralInt:
					{
						rows->insert(pair<Identifier, Value>(col_name, Value(expr->expr2->ival)));
						break;
					}	
					default:
						throw SQLExecError("unrecognized type");
					}
			}
			//Throw exception if predicate is other than "=". We can only handle equal
			//operator at this time
			else {
				throw SQLExecError("only equality predicates currently supported");
			}

		}
		//Throw exception if conjunction is other "AND". We can only handle AND
		//conjunction at this time
		else {
			throw SQLExecError("only support AND conjunctions");
		}

		return rows;
	}
	//Throw exception if no operators are found at all
	else {
		throw SQLExecError("No operator found");
	}
	
}

//insert a row into table
QueryResult *SQLExec::insert(const InsertStatement *statement) {

	Identifier tbname = statement->tableName;
	DbRelation& table = SQLExec::tables->get_table(tbname);
	ValueDict final_row;
	ColumnNames col_names;
	vector<Value> col_vals;
	IndexNames index_names;
	Handle insert_handle;
	unsigned int index_size = 0;
	
	//populate column values. We can only handle Text and Int at this time.
	for (auto const &expr : *statement->values) {
		switch (expr->type) {
		case kExprLiteralString:
			col_vals.push_back(Value(expr->name));
			break;
		case kExprLiteralInt:
			col_vals.push_back(Value(expr->ival));
			break;
		default:
			throw SQLExecError("Insert can only handle INT or TEXT");
		}
		
	}

	//populate column names. If SQL statement doesn't explicitly specify
	//column names, get the column names straight from the schema table
	if (statement->columns != nullptr) {
		for (char * column : *statement->columns) {
			col_names.push_back(column);
		}
	}
	else {
		for (auto const col: table.get_column_names()) {
			col_names.push_back(col);
		}
		
	}
	
	//create ValueDict of column name: column value
	for (unsigned int i = 0; i < col_names.size(); i++) {
		final_row[col_names[i]] = col_vals[i];
	}


	try {
		//Take that ValueDict and insert entry into table
		insert_handle = table.insert(&final_row);

		//update index table
		try {
			index_names = SQLExec::indices->get_index_names(tbname);
			index_size = index_names.size();
			for (unsigned int i = 0; i < index_names.size(); i++) {
				DbIndex& index = SQLExec::indices->get_index(tbname, index_names[i]);
				index.insert(insert_handle);
			}
		}
		//If a row cannot be inserted into the table, delete the index content referenced
		//to that row in index table
		catch (exception &e) {
			try {
				for (unsigned int i = 0; i < index_names.size(); i++) {
					DbIndex& index = SQLExec::indices->get_index(tbname, index_names[i]);
					index.del(insert_handle);
				}
			}
			catch (...) {

			}
			throw;
		}
		
	}
	//To throw back to main and display insertion error
	catch (exception &e) {
		throw;
	}

	//If all goes well, display a successful message
	string msg = "successfully inserted 1 row into " + tbname;
	if (index_size != 0)
		msg += " and " + to_string(index_size) + " indices";
	
	return new QueryResult(msg);  
}

//delete a row from a table
QueryResult *SQLExec::del(const DeleteStatement *statement) {

	Identifier tbname = statement->tableName;
	DbRelation& table = SQLExec::tables->get_table(tbname);
	ColumnNames col_names;

	//Get a list of all columns
	for (auto const col : table.get_column_names()) {
		col_names.push_back(col);
	}

	//Start base of plan at a TableScan
	EvalPlan *plan = new EvalPlan(table);

	//Enclose that in a Delete for where clause
	ValueDict* whereCondition = new ValueDict;
	if (statement->expr != NULL) {
		try {
			whereCondition = get_where_conjunction(statement->expr, &col_names);
		}
		catch (exception &e) {
			throw;
		}
		plan = new EvalPlan(whereCondition, plan);
		
	}

	//Optimize the plan and pipeline the optimized plan
	EvalPlan *optimized = plan->optimize();
	EvalPipeline pipeline = optimized->pipeline();

	//Remove index content referenced to this row. Since index delete operation
	//has not been implemented yet. We just added the try catch block to 
	//throw the exception
	auto index_names = SQLExec::indices->get_index_names(tbname);
	Handles *pipeline_handles = pipeline.second;
	unsigned int index_size = index_names.size();
	unsigned int handles_size = pipeline_handles->size();
	for (auto const& handle : *pipeline_handles) {
		try {
			for (unsigned int i = 0; i < index_names.size(); i++) {
				DbIndex& index = SQLExec::indices->get_index(tbname, index_names[i]);
				index.del(handle);
			}
		}
		catch (exception &e) {
			throw;
		}	
	}

	//Remove from table
	for (auto const& handle : *pipeline_handles) {
		table.del(handle);
	}
	
	//Handle memory
	delete whereCondition;

	//If all goes well, display a successful message
	string msg = "successfully deleted " + to_string(handles_size) +
		" rows from " + tbname;
	if (index_size != 0)
		msg += " and " + to_string(index_size) + " indices";

	return new QueryResult(msg);
}

//select entries from a table with or without where condition
QueryResult *SQLExec::select(const SelectStatement *statement) {

	TableRef *table_ref = statement->fromTable;
	Identifier tbname;
	ColumnNames* col_names = new ColumnNames;

	//get table name from parser and make sure the SQL statements are standard
	//select statements. We can't handle advanced select statements at this time
	switch (table_ref->type) {
	case kTableName:
		tbname = table_ref->name;
		break;
	default:
		throw SQLExecError("Can only handle SELECT * FROM table WHERE col_1 = 1 AND col_n = ""three"""
			" and SELECT col_1, col_2 FROM table");
	}

	//get column names from parser and make sure the SQL statements are standard
	//select statements. We can't handle advanced select statements at this time
	for (auto const &expr : *statement->selectList) {
		switch (expr->type) {
		case kExprStar:
			break;
		case kExprColumnRef:
			col_names->push_back(expr->name);
			break;
		default:
			return new QueryResult("Unable to handle this type of select");
		}
	}

	//get the table specified from the select statement
	DbRelation& table = SQLExec::tables->get_table(tbname);

	//get column in select *
	if (col_names->empty()) {
		for (auto const col : table.get_column_names()) {
			col_names->push_back(col);
		}
	}
	
	//Start base of plan at a TableScan
	EvalPlan *plan = new EvalPlan(table);

	//Enclose that in a Select if we have a where clause
	ValueDict* whereCondition = new ValueDict;
	if (statement->whereClause != NULL) {
		try {
			 whereCondition = get_where_conjunction(statement->whereClause, &table.get_column_names());
			
		}
		catch (exception &e) {
			throw;
		}
		plan = new EvalPlan(whereCondition, plan); 
	}

	//Wrap the whole thing in a ProjectAll or a Project
	plan = new EvalPlan(col_names, plan);

	//Optimize the plan and evaluate the optimized plan
	EvalPlan *optimized = plan->optimize();
	ValueDicts* rows = optimized->evaluate();

	//Handle memory
	delete whereCondition; 

	//If all goes well, display a successful message
	string msg = "successfully returned " + to_string(rows->size()) + " rows";
	return new QueryResult(col_names, NULL, rows, msg);  
}

//method helper used exclusively by create_table to get column attributes
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

//acts as a triage to call approrpriate create method 
QueryResult *SQLExec::create(const CreateStatement *statement) {
	switch (statement->type) {
	case CreateStatement::kTable:
		return create_table(statement);
	case CreateStatement::kIndex:
		return create_index(statement);
	default:
		return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
	}
}


//create table
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
	if (statement->type != CreateStatement::kTable)
		return new QueryResult("Only handling CREATE TABLE at the moment");

	// update _tables schema
	Identifier name = statement->tableName;
	ValueDict row;
	row["table_name"] = name;


	ColumnNames column_order;
	ColumnAttributes column_attributes;

	Identifier columnName;
	ColumnAttribute attribute;
	for (ColumnDefinition *column : *statement->columns)
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
		try {
			int count = 0;
			for (auto const& column_name : column_order)
			{
				row["column_name"] = column_name;

				switch (column_attributes[count].get_data_type())
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
			if (statement->ifNotExists)
				_tables.create_if_not_exists();
			else
				_tables.create();
		}
		catch (exception &e)
		{
			// attempt to undo the insertions into _columns
			try {

				for (auto const &h : columnHandles)
				{
					_columns.del(h);
				}
			}
			catch (exception &e)
			{

			}
			throw;
		}
	}
	catch (exception& e)
	{
		// attempt to undo the insertion into _tables
		try {
			SQLExec::tables->del(tableHandle);
		}
		catch (exception& e)
		{

		}
		throw;
	}

	return new QueryResult("Created " + name);
}

//create index
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
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

	for (Identifier columnName : *statement->indexColumns)
	{
		found = false;
		count = 0;

		for (auto const& tableColumn : table_column_order)
		{
			if (columnName == tableColumn)
			{
				found = true;
				column_order.push_back(columnName);
				column_attributes.push_back(table_column_attributes[count]);
				count++;
			}
		}
		if (!found)
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
	row["index_type"] = indexType;

	if (indexType == "BTREE")
		row["is_unique"] = true;
	else if (indexType == "HASH")
		row["is_unique"] = false;
	else
	{
		indexType = "BTREE";
		row["index_type"] = indexType;
		row["is_unique"] = true;
	}

	// Insert a row for each column in index key into _indices.
	// I recommend having a static reference to _indices in SQLExec, as we do for _tables.
	Handles iHandles;
	
	//Catching errror when inserting each row to _indices schema table
	try {
		for (auto const& column_name : column_order)
		{
			row["column_name"] = column_name;
			row["seq_in_index"] = seq_in_index++;
			iHandles.push_back(SQLExec::indices->insert(&row));
			count++;
		}

		// Call get_index to get a reference to the new index and then invoke the create method on it.
		DbIndex& index = SQLExec::indices->get_index(tableName, indexName);
		try {
			index.create();
		}
		catch (exception &e) {
			index.drop();
			throw;
		}	
	}
	catch (exception& e) {
		try {
			for (unsigned int i = 0; i < iHandles.size(); i++) {
				SQLExec::indices->del(iHandles.at(i));
			}
		}
		catch (...) {
		}

		throw;
	}
	


	return new QueryResult("Created index " + indexName);
}

//acts as a triage to call appropriate drop statement
QueryResult *SQLExec::drop(const DropStatement *statement) {
	switch (statement->type) {
	case DropStatement::kTable:
		return drop_table(statement);
	case DropStatement::kIndex:
		return drop_index(statement);
	default:
		return new QueryResult("Only DROP TABLE and DROP INDEX are implemented");
	}
}

//drop table
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
	if (statement->type != DropStatement::kTable) {
		throw SQLExecError("Unrecognized drop type!");
	} //from prompt

	Identifier name = statement->name;
	if (name == Tables::TABLE_NAME || name == Columns::TABLE_NAME || name == Indices::TABLE_NAME) {
		throw SQLExecError("Cannot drop the schema!");
	}

	ValueDict select_name;
	select_name["table_name"] = Value(name);

	DbRelation& table = SQLExec::tables->get_table(name);
	DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

	//get indices for deletion
	IndexNames all_indices = SQLExec::indices->get_index_names(name);
	for (auto const& index : all_indices) {
		DbIndex& to_drop = SQLExec::indices->get_index(name, index);
		Handles* temp_handles = SQLExec::indices->select(&select_name);
		to_drop.drop();
		for (auto const& handle : *temp_handles) {
			SQLExec::indices->del(handle);
		}
		delete temp_handles;
	}

	Handles* handles = columns.select(&select_name);

	for (auto const& row : *handles) {
		columns.del(row);
	}
	delete handles;

	table.drop(); //done in order per prompt
	SQLExec::tables->del(*SQLExec::tables->select(&select_name)->begin());
	return new QueryResult(string("Dropped ") + name);
}

//drop index
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
	Identifier index_name = statement->indexName;
	Identifier table_name = statement->name;

	DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
	ValueDict select_name;
	select_name["table_name"] = Value(table_name);
	select_name["index_name"] = Value(index_name);

	Handles* handles = SQLExec::indices->select(&select_name);
	index.drop();
	for (auto const& handle : *handles) {
		SQLExec::indices->del(handle);
	}

	delete handles;

	return new QueryResult("Dropped index: " + index_name);
}

//acts as a triage to call appropriate show statement
QueryResult *SQLExec::show(const ShowStatement *statement) {
	switch (statement->type)
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

//show index
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
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
	for (auto const& handle : *handles) {
		ValueDict* row = SQLExec::indices->project(handle, column_names);
		index_rows->push_back(row);
	}
	delete handles;

	message += "Successfully returned ";
	message += to_string(length);
	message += " rows";

	return new QueryResult(column_names, attributes, index_rows, message);
}

//show tables
QueryResult *SQLExec::show_tables() {
	std::string message;
	int count = 0;

	ColumnNames* names = new ColumnNames;
	names->push_back("table_name");

	ColumnAttributes* attributes = new ColumnAttributes;
	attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	Handles* handles = SQLExec::tables->select();

	ValueDicts* rows = new ValueDicts;
	for (auto const& handle : *handles) {
		ValueDict* row = SQLExec::tables->project(handle, names);

		if (row->at("table_name").s != Tables::TABLE_NAME &&
			row->at("table_name").s != Columns::TABLE_NAME &&
			row->at("table_name").s != Indices::TABLE_NAME)
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

//show columns
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
	for (auto const& index : *handles) {
		ValueDict* single_row = relation.project(index, names);
		rows->push_back(single_row);
	}
	delete handles;

	message = "Successfully returned " + to_string(length) + " rows";
	return new QueryResult(names, attributes, rows, message);
}
