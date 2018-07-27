/** 
 * PSC5300 Milestone1, sql5300.cpp
 * 
 * @author Greg Deresinski, Kevin Lundeen
 * @version Mileston1 8 Jul 2018
 */
#include <iostream>
#include <string.h>
#include "db_cxx.h"
#include "heap_storage.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

// Set project's home directory
const char *HOME = "sql5300/data";
const char *EXAMPLE = "example.db";
const unsigned int BLOCK_SZ = 4096;

// Forward declard
string expressionToString(const Expr* expr);
string operatorToString(const Expr* expr);

	/**
 * Convert the hyrise Expr AST back into the equivalent SQL
 * @param expr expression to unparse
 * @return     SQL equivalent to *expr
 */
	string
	expressionToString(const Expr *expr)
{
	string ret;
	switch (expr->type) {
	case kExprStar:
		ret += "*";
		break;
	case kExprColumnRef:
		if (expr->table != NULL)
			ret += string(expr->table) + ".";
	case kExprLiteralString:
		ret += expr->name;
		break;
	case kExprLiteralFloat:
		ret += to_string(expr->fval);
		break;
	case kExprLiteralInt:
		ret += to_string(expr->ival);
		break;
	case kExprFunctionRef:
		ret += string(expr->name) + "?" + expr->expr->name;
		break;
	case kExprOperator:
		ret += operatorToString(expr);
		break;
	default:
		ret += "???";  // in case there are exprssion types we don't know about here
		break;
	}
	if (expr->alias != NULL)
		ret += string(" AS ") + expr->alias;
	return ret;
}

/**
 * 
 * @param
 * @return
 */
//TODO from Nina, understand
string operatorToString(const Expr* expr) {
    if (expr == NULL)
        return "null";

    string ret;
	// Unary prefix operator: NOT
	if(expr->opType == Expr::NOT)
        ret += "NOT ";

    string left(expressionToString(expr->expr) + " ");

    switch (expr->opType) {
        case Expr::SIMPLE_OP:
        	ret += expr->opChar;
            break;
        case Expr::AND:
            ret += "AND";
            break;
        case Expr::OR:
            ret += "OR";
            break;
        case Expr::NOT:
            ret += "NOT";
            break;
        default:
            break; 
    }

	// Right-hand side of expression (only present for binary operators)
    if (expr->expr2 != NULL)
        ret += " " + expressionToString(expr->expr2);
    
    return ret;
}

/**
 * Convert the hyrise TableRef AST back into the equivalent SQL
 * @param table  table reference AST to unparse
 * @return       SQL equivalent to *table
 */
string tableRefInfoToString(const TableRef *table) {
	string ret;
	switch (table->type) {
	case kTableSelect:
		ret += "kTableSelect FIXME"; // FIXME
		break;
	case kTableName:
		ret += table->name;
		if (table->alias != NULL)
			ret += string(" AS ") + table->alias;
		break;
	case kTableJoin:
		ret += tableRefInfoToString(table->join->left);
		switch (table->join->type) {
		case kJoinCross:
		case kJoinInner:
			ret += " JOIN ";
			break;
		case kJoinOuter:
		case kJoinLeftOuter:
		case kJoinLeft:
			ret += " LEFT JOIN ";
			break;
		case kJoinRightOuter:
		case kJoinRight:
			ret += " RIGHT JOIN ";
			break;
		case kJoinNatural:
			ret += " NATURAL JOIN ";
			break;
		}
		ret += tableRefInfoToString(table->join->right);
		if (table->join->condition != NULL)
			ret += " ON " + expressionToString(table->join->condition);
		break;
	case kTableCrossProduct:
		bool doComma = false;
		for (TableRef* tbl : *table->list) {
			if (doComma)
				ret += ", ";
			ret += tableRefInfoToString(tbl);
			doComma = true;
		}
		break;
	}
	return ret;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col) {
	string ret(col->name);
	switch(col->type) {
	case ColumnDefinition::DOUBLE:
		ret += " DOUBLE";
		break;
	case ColumnDefinition::INT:
		ret += " INT";
		break;
	case ColumnDefinition::TEXT:
		ret += " TEXT";
		break;
	default:
		ret += " ...";
		break;
	}
	return ret;
}

/**
 * Execute a SQL SELECT statement. For now, just output SQL.
 * @param   stmt    Hyrise AST for SELECT statemetn
 * @returns a string of the SQL statement
 */
string executeSelect(const SelectStatement* stmt) {
    string ret("SELECT ");
    bool printComma = false;

    for (Expr* expr: *stmt->selectList) {
        expressionToString(expr);
        if(printComma)
            ret += ", ";
        ret += expressionToString(expr);
        printComma = true;
    }

    //TODO FROM and WHERE
    ret += " FROM ";
    ret += tableRefInfoToString(stmt->fromTable);

    if (stmt->whereClause != NULL)
        ret += " WHERE " + expressionToString(stmt->whereClause);
    return ret;
}

/**
 * Execute a SQL INSERT statement. For now, just outputs SQL
 * @param stmt  Hyrise AST for the INSERT statement
 * @returns a string of the SQL statement
 */
string executeInsert(const InsertStatement* stmt) {
    string ret("INSERT ...");

    return ret;
}

/**
 * Execute a SQL CREATE statement. For now just outputs SQL query
 * @param stmt  A Hyrise AST for the CREATE statement
 * @returns a string of the SQL statement
 */
string executeCreate(const CreateStatement* stmt) {
	string ret("CREATE TABLE ");
	if (stmt->type != CreateStatement::kTable)
		return ret + "...";
	if (stmt->ifNotExists)
		ret += "IF NOT EXISTS ";
	ret += string(stmt->tableName) + " (";
	bool doComma = false;
	for (ColumnDefinition *col : *stmt->columns)
	{
		if (doComma)
			ret += ", ";
		ret += columnDefinitionToString(col);
		doComma = true;
	}
	ret += ")";
	return ret;
}

/**
 * Executes SQL statement by printing parsed statement
 * @param stmt valid SQL statement from Highrise parser
 * @returns a string of the valid SQL syntax
 */
string execute(const SQLStatement* stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            return executeSelect((const SelectStatement*) stmt);
        case kStmtInsert:
            return executeInsert((const InsertStatement*) stmt);
        case kStmtCreate:
            return executeCreate((const CreateStatement*) stmt);
        default:
            return "Invalid SQL statement";
    }
}

/**
 * Main entry point.
 * @param envdir Directory for BerkeleyDB environment
 * @return int
 */
int main(int argc, char **argv) {
    if (argc!= 2) {
		cerr << "Usage: sql5300: dbpath" << endl;
		exit(-1);
	}
	string envdir = argv[1];

	DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	try {
		env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException &e) {
		cerr << "Error opening database environment: "
			  << envdir << endl;
		cerr << e.what() << endl;
		exit(-1);
	}
	
	// Print database environment
	cout << "Running database environemnt: " << envdir << "\n";
	
	// Start SQL> prompt
	while (true) {
		cout << "SQL> ";
		string query;
		getline(cin, query);
		if (query == "quit")
			break;	// End program
		else if (query == "test") {
			std::cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << std::endl;
			continue;
		}
		// Use Hyrise SQL parser to parse input    		
		SQLParserResult* result = SQLParser::parseSQLString(query);
		
        // Validate SQL statement
		if(result->isValid()) {
			cout << "Valid syntax" << endl;
			cout << "Number of statements is: " << result->size() << endl;

			// Parse SQL statement
			for (uint i = 0; i < result->size(); ++i) {
			    cout << execute(result->getStatement(i)) << endl;
			}
			delete result;
		} else {
			cout << "Invalid SQL syntax" << endl;
			delete result;
		}

	}
	return 0; 
}
