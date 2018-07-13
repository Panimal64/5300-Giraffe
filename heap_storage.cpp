/**
 * @file heap_storage.cpp
 * @author Greg Deresinski
 * @author Kevin Lundeen, prof cited within
 */
#include <iostream>
#include <utility>
#include <string.h>
#include "heap_storage.h"

typedef u_init16_t u16;
typedef u_init32_t u32;

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
        this->send_free = DbBlock::BLOCK_SZ - 1;
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
    this->send_free -= size;
    u16 loc - this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}
 
/**
 *  Retrieve a record from the block in unmarshalled format
 */
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size; 
    u16 offset;
    get_header(size, offset, record_id);
    if (offset = 0)
        return nullptr;
    else
        return new Dbt(this->address(), size);
}

/**
 *  Place a new record in the block
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
    u16 size; 
    u16 loc; 
    get_header(record_id, size, loc);
    u16 new_size = (u16)data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if(!has_room(extra)) {
            throw DbBlockNoRoomError("Not enough room in block");
        }
        this->slide(loc+newsize, loc+size);
        std::memcpy(this->address(loc-extra), data.get_data(), newSize);
    } else {
        std::memcpy(this->address(loc), data.get_data(), newSize);
        this->slide(loc+newsize, loc+size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, newSize, loc);
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

RecordIDs* SlottedPage::ids(void) {
    RecordIds* record_IDs = new RecordIDs();

    for (record_id : this->num_records) {
        u16 size;
        u16 loc;
        get_header(size, loc, record_id);
        if (loc != 0)
            record_IDs->push_back(record_id);
    }
    return record_IDs;
}

/**
 * @param size  size of record in block
 * @param loc   location of record in block
 * @param RecordID specified record id
 */
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = get_n(4*id);
    id = get_n(4*id + 2);
}

// Store the size and offset for a given id. For id of zero, store the block header.
void SlottedPage::put_header(RedcordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

/**
 * @param start of block
 * @param end of block
 * @return boolean indicating if block can fit
 */
bool SlottedPage::has_room(u16 start, u16 end) {
    u16 available = this->end_free - (u16)((this->num_records) + 1) * 4;
    return (size <= available);
}

/**
 * @param start of block
 * @param end of block
 */
void SlottedPage::slide(u16 start, u16 end) {
    u16 shift = end - start;
	if (shift == 0)
		return;
	std::memcpy(this->address(end_free + 1), this->address(end_free + 1 + shift), shift);
	u16 size;
    u16 loc;
	RecordIDs* record_IDs = ids();
	for (uint record : record_IDs) {
		this->get_header(size, loc, record_IDs);
        if( loc <= size){
            loc += shift;
            this->put_header(r_ids, size, loc);;
		}
	}

	this->end_free += shift;
    this->put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16)this->address(offset);
}

// Put a 2-byte integer at a given offset in block.
void SlottedPage::put_n(u16 offset) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

/**
 * @class HeapFile - heap file implementation of DbFile
 */
void class Heapfile : DbFile {
HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
}

/**
 * Create physical file.
 */
void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = get_new(); 
    this->put(block);
}

/**
 * Drops table, deletes the physical file.
 */
void HeapFile::drop(void) {
    //TODO drop table
    this->close();
    remove(this->dbfilename.cstr(), nullptr, 0);
}

/**
 * Open the physical file.
 */
void HeapFile::open(void) {
    this->db_open();
}

/**
 * Close the physical file.
 */
void HeapFile::close(void) {
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
    char block[DB_BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block, sizeof(block));

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
    Dbt key(&block, sizeof(block)); 
    this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Sequence of all block ids.
 */
BlockIDs* HeapFile::block_ids() {
    BlockIDs* blockIDs = new BlockIDs();
    for (auto i = 1; i < this->last; i++) {
        blockIDs->push_back(i);
    }
    return blockIDs;
}

/**
 * @return last block_id of the file.
 */
u32 HeapFile::get_last_block_id() {
    return last;
}

/**
 * Wrapper for Berkeley DB open, which does both open and creation.
 */
void HeapFile::db_open(uint flags=0) {
    const char* path;
    DB_BTREE_STAT *sp;
    if (!this->closed)
        return;
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    _DB_ENV->get_home(&path);
    this->dbfilename = _DB_ENV + this->name + ".db";
    this->db.open(this->dbfilename,nullptr,DB_RECNO,0);
    this->last = db.stat(nullptr, DB_FAST_STAT);
    this->closed = false;


/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attribtues) 
    : DbRelatation(table_name, column_names, column_attribtues)
{
   
}

/**
 *  Executes CREATE_TABLE table_name { cols }
 */
void HeapTable::create() {
    this->file.create();
}

/**
 *  If table doesn't exist, create a new one
 */
void HeapTable::create_if_not_exists() {
    try {
        this->file.open();
        throw DbNoSuchFileError
    } catch (DbNoSuchFileError) {
        this->file.create(); 
    }
}

/**
 * Executes a DROP TABLE table_name
 */
void HeapTable::drop() {
    this->file.delete();
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
    this->file.open();
    return append(validate(this->row));
}

/**
 * Executes UPDATE INFO table_name SET new_values WHERE handle
 * where handle is sufficient to identify one specific record
 */
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    //TODO
    throw TypeError("FIXME");
}

/**
 * Executes DELETE FROM table_name WHERE handle
 */
//TODO Do we need to implement?
void HeapTable::del(const Handle handle) {
    throw TypeError("FIXME");
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
    Dbt* data = bock->get(record_id);
    ValueDict* row = unmarshal(data);
    return row;
}

/**
 * Returns specified fields from a row
 * @param handle block id, record id pair
 * @param column_names specified column names
 * @return a sequence of values for handle given by column_names.
 */
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
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
}


/**
 * Check if the given row is acceptable to insert. Raise ValuerError if not.
 * @param ValueDict of row
 * @return ValueDict of validated row
 */
ValueDict* HeapTable::validate(const ValueDict* row) {
    ValueDict* full_row = new ValueDict();
    for (uint const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (row->find(column_name) == row->end()) {
            return nullptr;
        } else {
            full_row->insert(std::pair<Identifier, Value>(ca->first, ca->second));
        }        
    }
    return full_row;

}

/**
 * Assumes row is fully fleshed-out. Appends a record to thie file.
 * @return a Handle (a key-value pair of the value written and its loc)
 */
Handle HeapTable::append(const ValueDict* row) {
    Dbt* data = this->marshal(row);
    RecordID record_ID;
    SlottedPage* block = this->file.get(get_last_block_id);
    try {
        record_ID = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        block = this->file.get_new();
        record_ID = block->add(data);
    }
    Handle handle = new Handle();
    return handle;
}

/**
 * Caller responsible for freeing the returned Dbt and its enclodes ret->get_data().
 * @author Kevin Lundeen
 * @return a Dbt of the actual bits in a file
 */
Dbt* HeapTable::marshal(const ValueDict* row) {
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
ValueDict* HeapTable::unmarshal(Dbt* data) {
    char* bytes = (char*)data->get_data();
    ValueDict* row = new ValueDict();
    uint offset = 0;
    for (uint const& column_name: this->column_name) {
        ColumnAttribute ca = this->column_attribute[col_num++];
        if (ca.get_data_type() == ColumnAttribute:DataType::INT) {
            int32_t value = *(int32_t) (bytes + offset);
            row->insert(std::pair<Identifier, Value>(column_name, value));
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute.DataTyep::TEXT) {
            u16 size = *(u16) (bytles + offset);
            char* buffer = new char[size];
            offset += sizeof(u16);
            std::memcpy((void*)buffer, (bytes + offset), size);
            row->insert(std::pair<Identifier, Value>(column_name, value));
            offset += size;
        } else {
            throw DbRelationError("Cannot unmarshal ")
        }
    }
}


/**
 * Copy of Kevin Lundeen's test function for heap_storage classes
 * @authore Kevin Lundeen
 * @returns boolean indicating passed tests
 */
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
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
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

    return true;
}

