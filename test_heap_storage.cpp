// test function -- returns true if all tests pass

//taken from kLundeen for accurate testing purposes
bool test_compare(DbRelation &table, Handle handle, int a, string b) {
    ValueDict *result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
	delete result;
	return false;
    }
    value = (*result)["b"];
    delete result;
    return !(value.s != b);
}i




bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    std::string b = "alkjsl;kj; as;lkj;alskjf;laks df;alsdkjfa;lsdkfj ;alsdfkjads;lfkj a;sldfkj a;sdlfjk a";
    row["a"] = Value(-1);
    row["b"] = Value(b);
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b)){
	return false;
    }
    std::cout << "select/project ok " << handles->size() << std::endl;
    delete handles;

    Handle last_handle;
    for(int i =0; i < 1000; i++){
	row["a"] = i;
	row["b"] = b;
	last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001){
	return false;
    }
    for (auto const& handle: *handles){
	if(!test_compare(table, handle, i++, b)){
	    return false;
	}
    }
    std::cout << "many inserts/select/projects ok" << endl;
    delete handles;
    
    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000){
	return false;
    }
    i = -1;
    for (auto const& handle: handles){
	if(!test_compare(table, handle, i++, b)){
	    return false;
	}
    }
    cout << "del ok" << endl;

    
    table.drop();
	delete handles;

    return true;
}
