/**
 * @file heap_storage.cpp
 * @author Greg Deresinski
 * @author Kevin Lundeen, prof cited within
 */
#include <iostream>
#include <utility>
#include <string.h>
#include "heap_storage.h"

typedef u_int16_t u16;
typedef u_int32_t u32;


DbEnv* _DB_ENV;

/**
 * @class SlottedPage - heap file implementation of DbBlock
 *
 */

/**
 * Constructor
 * @author Kevin Lundeen
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
  if (is_new) {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  } else {
    get_header(this->num_records, this->end_free);
  }
}

/**
 * @param Dbt *data
 * @return RecordID a new record to the block. Return its id.
 */
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
  if (!has_room(data->get_size()))
    throw DbBlockNoRoomError("not enough room for new record");
  u16 id = ++this->num_records;
  u16 size = (u16) data->get_size();
  this->end_free -= size;
  u16 loc = this->end_free + 1;
  put_header();
  put_header(id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return id;
}

/**
 *  Retrieve a record from the block in unmarshalled format
 */
Dbt* SlottedPage::get(RecordID record_id) const {
  u16 size;
  u16 offset;
  get_header(size, offset, record_id);
  if (offset == 0)
    return nullptr;
  else
    return new Dbt(this->address(offset), size);
}

/**
 *  Place a new record in the block
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
  u16 size;
  u16 loc;

  get_header(size, loc, record_id);

  //can we only insert same sized data wtihout having to move things
  //Smaller data would fit I guess but that would leave empty space
  if(data.get_size() != size)
    {
      throw DbBlockNoRoomError("not enough room to add data");
    }

  //set the header at the end
  put_header(record_id,size,loc);
  // updates block with the data
  memcpy(this->address(loc),data.get_data(),size);
  /*
    u16 new_size = (u16)data.get_size();
    if (new_size > size) {
    u16 extra = new_size - size;
    if(!has_room(extra)) {
    throw DbBlockNoRoomError("Not enough room in block");
    }
    this->slide(loc+new_size, loc+size);
    memcpy(this->address(loc-extra), data.get_data(), new_size);
    } else {
    memcpy(this->address(loc), data.get_data(), new_size);
    this->slide(loc+new_size, loc+size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
  */
}

/**
 * @param RecordID of block to be deleted
 *  Remove a record from the block. Record actually remains, but header is zeroed out.
 */
void SlottedPage::del(RecordID record_id) {
  u16 size = this->num_records;
  u16 loc = this->end_free;
  get_header(size, loc, record_id);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);
}

/**
 */

RecordIDs* SlottedPage::ids(void) const {
  RecordIDs *record_IDs = new RecordIDs();

  for (int record_id = 1; record_id <= this->num_records; record_id++)
    {
      u16 size;
      u16 loc;
      get_header(size, loc, record_id);
      if (size != 0)
        record_IDs->push_back(record_id);
    }
  return record_IDs;
}

/**
 * @param size  size of record in block
 * @param loc   location of record in block
 * @param RecordID specified record id
 */
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) const
{
  size = get_n(4*id);
  loc = get_n(4*id + 2);
}

//Stores the id and size of header to a location
//Defaults to store the header of the entire block when using put_header
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc){
  //when default
  if(id == 0)
    {
      size = this->num_records;
      loc = this->end_free;
    }
  //sets the size to the address of the header for the record
  put_n(4*id, size);
  //sets the two bytes after the size
  put_n(4*id +2, loc);

}

/**
 * @param start of block
 * @param end of block
 * @return boolean indicating if block can fit
 */
bool SlottedPage::has_room(u16 start) const{
  u16 available = (this->end_free - (u16)((this->num_records) + 1)) * 4;
  return (start <= available);
}

/**
 * @param start of block
 * @param end of block
 */
void SlottedPage::slide(u16 start, u16 end) {
  u16 shift = end - start;
  if (shift == 0)
    return;

  RecordIDs* records = this->ids();

  for(RecordID record_id : *records)
    {
      u_int16_t size;
      u_int16_t loc;
      get_header(size,loc,record_id);

    }

  /*memcpy(this->address(end_free + 1), this->address(end_free + 1 + shift), shift);
    u16 size;
    u16 loc;


    RecordIDs* record_IDs = ids();
    for (auto const &record_id : *record_IDs)
    {
    get_header(size, loc, record_id);
    if (loc <= start)
    {
    loc += shift;
    put_header(record_id, size, loc);
    }
    }

    this->end_free += shift;
    this->put_header();
  */
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) const{
  return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at a given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
  *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) const{
  return (void*)((char*)this->block.get_data() + offset);
}

/**
 * @class HeapFile - heap file implementation of DbFile
 */
 HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
 	this->dbfilename = this->name + ".db";
 }


/**
 * Create physical file.
 */
void HeapFile::create(void) {
  db_open(DB_CREATE | DB_EXCL);
  SlottedPage* block = get_new();

  this->put(block);
}

/**
 * Drops table, deletes the physical file.
 */
void HeapFile::drop(void) {
  //TODO drop table
  this->close();
  remove(this->dbfilename.c_str());
}

/**
 * Open the physical file.
 */
void HeapFile::open(void) {
  db_open();
}

/**
 * Close the physical file.
 */
void HeapFile::close(void){
  this->db.close(0);
  this->closed = true;
}

/**
 * Allocate a new block for the database file.
 * Return the new empty DbBlock that is managing the records in this block and its block id.
 *
 * @author  Kevin Lundeen
 * @return SlottedPage block
 */
SlottedPage* HeapFile::get_new(void) {
  char block[DbBlock::BLOCK_SZ];
  memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  int block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));

  // write out an empty block and read it back in so Berkeley DB is managing the memory
  SlottedPage* page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data, 0);  // write it out with initialization applied
  this->db.get(nullptr, &key, &data, 0);
  return page;
}

/**
 * Get a block from the database file.
 */
SlottedPage* HeapFile::get(BlockID block_id) {
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);
  SlottedPage* page = new SlottedPage(data, block_id);
  return page;
}

/**
 * Write a block back to the database file.
 */
void HeapFile::put(DbBlock* block) {
  BlockID blockID = block->get_block_id();
  Dbt key(&blockID, sizeof(blockID));
  this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Sequence of all block ids.
 */
BlockIDs* HeapFile::block_ids() const {
  BlockIDs* blockIDs = new BlockIDs();
  for (unsigned i = 1; i <= this->last; i++) {
    blockIDs->push_back(i);
  }
  return blockIDs;
}


/**
 * Wrapper for Berkeley DB open, which does both open and creation.
 */
void HeapFile::db_open(uint flags)
{
  if (!this->closed)
    return;
  this->db.set_re_len(DbBlock::BLOCK_SZ);
  // _DB_ENV->get_home(&path);
  this->dbfilename = this->name + ".db";
  this->db.open(nullptr, this->dbfilename.c_str(), this->name.c_str(), DB_RECNO, (u_int32_t)flags, 0);
  // this->last = db.stat(nullptr, &sp, DB_FAST_STAT);
  this->closed = false;
}


uint32_t HeapFile::get_block_count() {
	DB_BTREE_STAT* stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	return stat->bt_ndata;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}

/**
 *  Executes CREATE_TABLE table_name { cols }
 */
void HeapTable::create()
{
  try{
    file.create();
  }
  catch(DbException &e)
    {
      throw DbRelationError("TABLE IS ALREADY CREATED");
    }
}

/**
 *  If table doesn't exist, create a new one
 */
void HeapTable::create_if_not_exists() {
  try {
    this->file.open();
  }
  catch (DbException &e)
    {
      this->file.create();
    }
}

/**
 * Executes a DROP TABLE table_name
 */
void HeapTable::drop() {
  this->file.drop();
}

/**
 * Open existing table.
 * Enables: insert, update, delete, select, project
 */
void HeapTable::open() {
  this->file.open();
}

/**
 * Closes the table.
 * Disables: insert, update, delete, select, project
 */
void HeapTable::close() {
  this->file.close();
}

/**
 * Executes INSERT INTO table_name (row_keys) VALUES (row_values)
 * @return a Handle of the inserted row
 */
Handle HeapTable::insert(const ValueDict* row) {
  open();

  return append(validate(row));
}

/**
 * Executes UPDATE INFO table_name SET new_values WHERE handle
* where handle is sufficient to identify one specific record
*/
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
  //TODO
  throw DbRelationError("Not Implemented");
}

/**
 * Executes DELETE FROM table_name WHERE handle
 */
//TODO Do we need to implement?
void HeapTable::del(const Handle handle) {
  throw DbRelationError("Not Implemented");
}

/*
 *  Corresponds to SELECT * FROM
 *
 *  Returns handles to the matching rows.
 */
Handles* HeapTable::select() {
  Handles* handles = new Handles();

  BlockIDs* block_ids = file.block_ids();

  for (auto const& block_id: *block_ids) {
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();

    // returns ALL Rows
    for (auto const& record_id: *record_ids)
      handles->push_back(Handle(block_id, record_id));

    delete record_ids;
    delete block;
  }

  delete block_ids;

  return handles;
}

/**
 * @author Kevin Lundeen
 * Executes SELECT handle FROM table_name WHERE where
 * @return a Handle of query result
 */
Handles* HeapTable::select(const ValueDict* where) {
  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();
  for (auto const& block_id: *block_ids) {
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    for (auto const& record_id: *record_ids)
      handles->push_back(Handle(block_id, record_id));
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}

/**
 * Returns all fields of a given handle
 * @param Handle
 * @return ValueDict of data fields
 */
ValueDict* HeapTable::project(Handle handle) {
  BlockID block_id = handle.first;
  RecordID record_id = handle.second;
  SlottedPage *block = this->file.get(block_id);
  Dbt* data = block->get(record_id);
  ValueDict* row = unmarshal(data);

  delete block;
  delete data;

  return row;
}

/**
 * Returns specified fields from a row
 * @param handle block id, record id pair
 * @param column_names specified column names
 * @return a sequence of values for handle given by column_names.
 */
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
  // Get block id from handle
  BlockID block_id = std::get<0>(handle);
  // Get record id from handle;
  RecordID record_id = std::get<1>(handle);

  // Retrieve the page from the block provided by handle
  SlottedPage* page = file.get(block_id);

  // Retrieve the record from the page
  Dbt* record = page->get(record_id);

  // Unmarshal data for returning
  ValueDict* retrievedVal = unmarshal(record);
  ValueDict* returnVal = new ValueDict;
  for(auto const& column_name: *column_names)
    {
      ValueDict::const_iterator column = retrievedVal->find(column_name);
      Value value = column->second;
      returnVal->insert(std::pair<Identifier, Value>(column_name, value));

    }

  // Free Memory
  delete record;
  delete page;
  delete retrievedVal;

  return returnVal;
  /*
    ValueDict *result = new ValueDict();
    ValueDict *row = project(handle);
    if (column_names == NULL){
  return row;
}
    else{
  for (auto const &column_name : *column_names){
  if (row->find(column_name) != row->end())
    result->insert(std::pair<Identifier, Value>(column_name, row->at(column_name)));
}
}
    return result;
    */
}


/**
 * Check if the given row is acceptable to insert. Raise ValuerError if not.
 * @param ValueDict of row
 * @return ValueDict of validated row
 */
ValueDict* HeapTable::validate(const ValueDict* row) const{
  ValueDict* full_row = new ValueDict();
  uint col_num = 0;

  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;

    // If data type not recognized throw error, else push it
    if (ca.get_data_type() != ColumnAttribute::DataType::INT && ca.get_data_type()
        != ColumnAttribute::DataType::TEXT) {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
    else
      {
        full_row->insert(std::pair<Identifier, Value>(column_name, value));
      }
  }

  /*
    for (auto const& column_name: this->column_names)
    {
    Value value;
    // ValueDict::const_iterator column = row->find(column_name);
    if (column == row->end())
    throw DbRelationError("NULL not supported");
    else
    value = column->second;
    (*full_row)[column_name] = value;
    }
  */
  return full_row;
}

/**
 * Assumes row is fully fleshed-out. Appends a record to thie file.
 * @return a Handle (a key-value pair of the value written and its loc)
 */
Handle HeapTable::append(const ValueDict* row) {
  Dbt* data = marshal(row);
  RecordID record_ID;
  SlottedPage *block = file.get(file.get_last_block_id());
  try {
    record_ID = block->add(data);
  } catch (DbBlockNoRoomError &e) {
    delete block;
    block = this->file.get_new();
    record_ID = block->add(data);
  }

  file.put(block);

  return Handle(this->file.get_last_block_id(), record_ID);
}

/**
 * Caller responsible for freeing the returned Dbt and its enclodes ret->get_data().
 * @author Kevin Lundeen
 * @return a Dbt of the actual bits in a file
 */
Dbt* HeapTable::marshal(const ValueDict* row) const{
  char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
  uint offset = 0;
  uint col_num = 0;
  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      *(int32_t*) (bytes + offset) = value.n;
      offset += sizeof(int32_t);
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      uint size = value.s.length();
      *(u16*) (bytes + offset) = size;
      offset += sizeof(u16);
      memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
      offset += size;
    } else {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

/**
 * Converts Dbt data into a ValueDict row
 * @param Dbt of data
 * @return ValueDict of data
 */
ValueDict* HeapTable::unmarshal(Dbt* data) const{
  ValueDict *row = new ValueDict();
  Value value;
  char *bytes = (char *)data->get_data();
  uint offset = 0;
  uint col_num = 0;
  for (auto const &column_name : this->column_names)
    {
      ColumnAttribute ca = this->column_attributes[col_num++];
      if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
          int32_t *container = new int32_t;
          memcpy(container, (const int32_t *) bytes + offset, sizeof(int32_t));
          value.n = *container;
          offset += sizeof(int32_t);

          delete container;
          /*
            value.n = *(int32_t *)(bytes + offset);
            offset += sizeof(int32_t);
          */
        }
      else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
          u16 size = *(u16 *)(bytes + offset);
          offset += sizeof(u16);
          char buffer[DbBlock::BLOCK_SZ];
          memcpy(buffer, bytes + offset, size);
          buffer[size] = '\0';
          value.s = std::string(buffer); // assume ascii for now
          offset += size;
        }
      else
        {
          throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
      (*row)[column_name] = value;
    }
  return row;
}

// See if the row at the given handle satisfies the given where clause
bool HeapTable::selected(Handle handle, const ValueDict* where) {
	if (where == nullptr)
		return true;
	ValueDict* row = this->project(handle, where);
	return *row == *where;
}

void test_set_row(ValueDict &row, int a, std::string b) {
	row["a"] = Value(a);
	row["b"] = Value(b);
}

bool test_compare(DbRelation &table, Handle handle, int a, std::string b) {
	ValueDict *result = table.project(handle);
	Value value = (*result)["a"];
	if (value.n != a) {
		delete result;
		return false;
	}
	value = (*result)["b"];
	delete result;
	return !(value.s != b);
}



/**

 * Copy of Kevin Lundeen's test function for heap_storage classes
 * @authore Kevin Lundeen
 * @returns boolean indicating passed tests
 */
bool test_heap_storage()
{
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
  table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
  std::cout << "drop ok" << std::endl;

  HeapTable table("_test_data_cpp", column_names, column_attributes);
  table.create_if_not_exists();
  std::cout << "create_if_not_exsts ok" << std::endl;

  ValueDict row;
  row["a"] = Value(12);
  row["b"] = Value("Hello!");
  std::cout << "try insert" << std::endl;
  table.insert(&row);
  std::cout << "insert ok" << std::endl;
  Handles *handles = table.select();
  std::cout << "select ok " << handles->size() << std::endl;
  ValueDict *result = table.project((*handles)[0]);
  std::cout << "project ok" << std::endl;
  Value value = (*result)["a"];
  if (value.n != 12)
    return false;
  value = (*result)["b"];
  if (value.s != "Hello!")
    return false;
  table.drop();

  delete handles;
  delete result;
  return true;
}
