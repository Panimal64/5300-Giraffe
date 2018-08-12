/**
* @file btree.cpp - implementation of B+Tree.B+ Trees
* While technically they are B+ trees, we use the more typical "BTree" terminology.
* @author Kevin Lundeen, Minh Nguyen, Dave Pannu
* @see "Seattle University, CPSC5300, Summer 2018"
*/
#include "btree.h"
#include <string>
#include <map>
using namespace std;

/**The B+ Tree index.
Only unique indices are supported.Try adding the primary key value to the index key to make it unique,
if necessary. Only insertion and lookup for the moment.
*/
BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	build_key_profile();
}

/**Figure out the data types of each key component and encode them in self.key_profile, 
a list of int / str class.
*/
void BTreeIndex::build_key_profile() {
	for (ColumnAttribute col_attr: *relation.get_column_attributes(this->key_columns)) {
		key_profile.push_back(col_attr.get_data_type());
	}

}

/**Free up stuff*/
BTreeIndex::~BTreeIndex() {
	delete this->stat;
	delete this->root;
	this->stat = nullptr;
	this->root = nullptr;
	
}

/**Create the index.*/
void BTreeIndex::create() {
	this->file.create();
	this->stat =  new BTreeStat(file, STAT, STAT + 1, key_profile);
	this->root = new BTreeLeaf(file, stat->get_root_id(), key_profile, true);
	this->closed = false;

	//Build the index- add every row from relation into index
	for (auto const handle : *relation.select()) {
		insert(handle);
	}
}

/**Drop the index.*/
void BTreeIndex::drop() {
	file.drop();
}

/**Open existing index. Enables: lookup, range, insert, delete, update.*/
void BTreeIndex::open() {
	if (this->closed) {
		file.open();
		this->stat = new BTreeStat(file, STAT, key_profile);
		if (this->stat->get_height() == 1) {
			this->root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);
		}
		else {
			this->root = new  BTreeInterior(file, stat->get_root_id(), key_profile, false);
		}
		this->closed = false;
	}
	
}

/**Closes the index. Disables: lookup, range, insert, delete, update.*/
void BTreeIndex::close() {
	file.close();
	this->stat = nullptr;
	this->root = nullptr;
	this->closed = true;
}

/** Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
 names in the index. Returns a list of row handles.*/
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	return _lookup(root, stat->get_height(), tkey(key_dict));
}

/**Recursive lookup*/
Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
	Handles* handles = new Handles;
	if (height == 1) {
		BTreeLeaf *leaf_node = (BTreeLeaf*)node;
		try{
			Handle handle = leaf_node->find_eq(key);
			handles->push_back(handle);
		}
		//To handle null result from lookup
		catch (std::out_of_range) {}
		return handles;	
	}
	else {
		BTreeInterior *interior_node = (BTreeInterior*)node;
		return _lookup(interior_node->find(key, height),
			height - 1, key);
	}
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME: Not in scope of M6
}

/**Insert a row with the given handle. Row must exist in relation already.*/
void BTreeIndex::insert(Handle handle) {
	KeyValue* keyval = tkey(relation.project(handle, &this->key_columns));
	Insertion split_root = _insert(this->root,
		this->stat->get_height(), keyval, handle);
	
	//If we split the root grow the tree up one level
	if (!root->insertion_is_none(split_root)) {
		BTreeInterior *root = new BTreeInterior(file, 0, key_profile, true);
		root->set_first(this->root->get_id());
		root->insert(&split_root.second, split_root.first);
		root->save();
		this->stat->set_root_id(root->get_id());
		this->stat->set_height(this->stat->get_height() + 1);
		this->stat->save();
		delete this->root;
		this->root = nullptr;
		this->root = root;
	}

}

/**Recursive insert. If a split happens at this level, 
return the (new node, boundary) of the split.*/
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
	//Base case: a leaf node. Insert method handles splitting leaf automatically. Don't
	//have to account for case that a node is too full to insert
	if (height == 1) {
			BTreeLeaf *leaf_node = (BTreeLeaf*)node;
			Insertion insert;
			insert = leaf_node->insert(key, handle);
			leaf_node->save();
			return insert;
			
	}
	else {
		//recursive case
		BTreeInterior *interior_node = (BTreeInterior*)node;
		Insertion new_kid = _insert(interior_node->find(key, height)
			, height - 1, key, handle);
		if (!node->insertion_is_none(new_kid)) {
			//Insert method handles splitting node automatically. Don't have to
			//account for case that a node is too full to insert
			Insertion insert = interior_node->insert(&new_kid.second, new_kid.first);
			interior_node->save();
			return insert;
		}
		return node->insertion_none();
	}
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME: Not in scope of M6
}

/**pull out the key values from the ValueDict in order*/
KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
	KeyValue* val = new KeyValue;
	for (Identifier col : this->key_columns) {
		val->push_back(key->at(col));
	}
	return val;

}

/**To test B-Tree implementation (insert and lookup)*/
bool test_btree() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	HeapTable table1("_test_btree_cpp", column_names, column_attributes);
	table1.create();
	bool result = true;

	ValueDict *row1 = new ValueDict;
	(*row1)["a"] = 12;
	(*row1)["b"] = 99;
	table1.insert(row1);
	ValueDict *row2 = new ValueDict;
	(*row2)["a"] = 88;
	(*row2)["b"] = 101;
	table1.insert(row2);

	for (unsigned int i = 0; i < 1000; i++) {
		ValueDict batch_row;
		batch_row["a"] = i + 100;
		batch_row["b"] = -i;
		table1.insert(&batch_row);
	}

	ColumnNames indx_col;
	indx_col.push_back(column_names.at(0));
	BTreeIndex indx(table1, "fooindex", indx_col, true);
	
	indx.create();
	
	
	ValueDict *test_row = new ValueDict;
	
	(*test_row)["a"] = 12;
	if (!testbtree_compare(indx, table1, test_row, row1)) {
		result = false;
	}
	
	(*test_row)["a"] = 88;
	if (!testbtree_compare(indx, table1, test_row, row2)) {
		result = false;
	}

	(*test_row)["a"] = 6;
	ValueDict *empty_row = new ValueDict;
	if (!testbtree_compare(indx, table1, test_row, empty_row)) {
		result = false;
	}

	for (unsigned int j = 0; j < 10; j++) {
		for (unsigned int i = 0; i < 1000; i++) {
			(*test_row)["a"] = i + 100;
			(*test_row)["b"] = -i;
			if (!testbtree_compare(indx, table1, test_row, test_row)) {
				result = false;
			}
		}
	}

	indx.drop();
	table1.drop();

	delete row1;
	delete row2;
	delete test_row;
	delete empty_row;
	return result;

}

/**Helper function to compare exepected and returned results*/
bool testbtree_compare(BTreeIndex &indx, HeapTable &table1, ValueDict *test, ValueDict *compare) {
	ValueDicts* result = new ValueDicts;
	Handles* indx_handles = indx.lookup(test);
	if (!indx_handles->empty()) {
		for (Handle& h : *indx_handles) {
			result->push_back(table1.project(h));
		}
	}
	
	//Check if both are empty. If so, they are equal
	if (result->empty() && compare->empty()) {
		delete result;
		return true;
	}
	
	//Check if one is empty and the other is not. If so, they are not equal
	if (result->empty() || compare->empty()) {
		delete result;
		return false;
	}

	//If neither the above cases, compare the contents
	for (ValueDict * result_index : *result) {
		for (auto const& entry : *compare) {
			if (entry.second != (*result_index)[entry.first]) {
				delete result;
				return false;
			}
		}
	}

		
	delete result;
	return true;
}

