/**
 * @file heap_storage.cpp
 * @author Greg Deresinski
 */
#include <map>
#include <string.h>
#include "heap_storage.h"

typedef u_init16_t u16;
typedef u_init32_t u32;

/**
 * @class SlottedPage - heap file implementation of DbBlock
 * 
 */
void SlottedPage : DbBlock {
// Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->send_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

//TODO destructor and deletes
SlottedPage::~SlottedPage() {
    //TODO
}

// Add a new record to the block. Return its id.
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
 
// Retrieve a record from the block in unmarshalled format
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size; 
    u16 offset;
    get_header(size, offset, record_id);
    if (offset = 0)
        return nullptr; //TODO if no record found, return NULL
    else
        return new Dbt(this->address(), size);
}

//TODO Place a new record in the block
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
    u16 size = this->num_records; //TODO
    u16 loc = this->end_free; //TODO
    put_header(record_id, size, loc);
    //TODO
}

//TODO Remove a record from the block. Record actually remains, but header is zeroed out.
void SlottedPage::del(RecordID record_id) {
    u16 size = this->num_records; //TODO
    u16 loc = this->end_free; //TODO
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

//TODO
RecordIDs* SlottedPage::ids(void) {
    //TODO
}

//TODO Start protected meothods
// u16 num_records;
// u16 end_free;

//TODO
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

//TODO 
bool SlottedPage::has_room(u16 start, u16 end) {
    //TODO
    if () {
        return true;
    } else {
        return false;
    }
}

//TODO
void SlottedPage::slide(u16 start, u16 end) {
    //TODO
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
};

/**
 * @class HeapFile - heap file implementation of DbFile
 */
void class Heapfile : DbFile {
//TODO Constructor
HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    //TODO
}

//TODO destuctor and deletes

//TODO
void HeapFile::create(void) {
    //TODO
}

//TODO
void HeapFile::drop(void) {
    //TODO
}

//TODO
void HeapFile::open(void) {
    //TODO
}

//TODO
void HeapFile::close(void) {
    //TODO
}

// Allocate a new block for the database file.
// Return the new empty DbBlock that is managing the records in this block and its block id.
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

//TODO
SlottedPage* HeapFile::get(BlockID block_id) {
    return SlottedPage*; //TODO
}

//TODO
void HeapFile::put(DbBlock* block) {
    //TODO
}

//TODO
BlockIDs* HeapFile::block_ids() {
    //TODO
}

//TODO
u32 HeapFile::get_last_block_id() {
    return last;
}

//TODO protected
//std::string dbfilename;
//u32 last;
//bool closed;
//Db db;

//TODO
void HeapFile::db_open(uint flags=0) {
    //TODO
}
};


/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
void class HeapTable : DbRelation {
//Constructor
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attribtues) 
    : DbRelatation(table_name, column_names, column_attribtues)
{
    
}

//TODO Destructor and deletes
HeapTable::~HeapTable() {

}

// Executes CREATE_TABLE table_name { cols }
void HeapTable::create() {
    this->file.create();
}

// If table doesn't exist, create a new one
void HeapTable::create_if_not_exists() {
    try {
        this->file.open();
        throw DbNoSuchFileError
    } catch (DbNoSuchFileError) {
        this->file.create(); 
    }
}

// Executes a DROP TABLE table_name
void HeapTable::drop() {
    this->file.delete();
}

// Open an existing table
void HeapTable::open() {
    this->file.open();
}

// Closes current table
void HeapTable::close() {
    this->file.close();
}

//TODO
Handle HeapTable::insert(const ValueDict* row) {
    this->file.open();
    return append(validate(this->row));
}

//TODO
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    //TODO
}

//TODO
void HeapTable::del(const Handle handle) {
    //TODO
}

// Executes SELECT handle FROM table_name WHERE where
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
    }
    delete record_ids;
    delete handles;
}

//TODO
ValueDict* HeapTable::project(Handle handle) {
    //TODO
}

//TODO Return a sequence of values for handle given column_names
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
    this->file.open();
    auto block_id = this->handle.block_id;
    auto record_id = this->handle.record_id;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block.get(record_id); //TODO
    ValueDict* row = unmarshal(data); //TODO use C++ map
    if (column_names == NULL)
        return row;
    else {
        for (auto const& column_name : *column_names)
            //TODO std::map<column_name, row->?>
        return ; //TODO
    }
}

// Heapfile file;

// Check if the given row is acceptable to insert. Raise ValuerError if not.
ValueDict* HeapTable::validate(const ValueDict* row) {
    //TODO full_row = {}
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (column_name) //TODO if column_name not in row:
            throw DbRelationError("Program does not handle NULLS");
        else {
            Value value = row[column_name];
        } //TODO if ('validate' in column)
            throw DbRelationError("Value is unaccaptable");
        
    }
    return //TODO

}

//TODO
Handle HeapTable::append(const ValueDict* row) {
    //TODO
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
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

//TODO 
ValueDict* HeapTable::unmarshal(Dbt* data) {
    ValueDict* value_dict = new ValueDict();
    uint offset = 0;
    for (auto const& column_name: this->column_name) {
        ColumnAttribute ca = this->column_attribute[col_num++];
        //TODO
        if (ca.get_data_type() == ColumnAttribute:DataType::INT) {
            int32_t value = *(int32_t) (bytes + offset);
            offset += sizeof(int32_t);
            //TODO
        } else if (ca.get_data_type() == ColumnAttribute.DataType::BOOLEAN) {
            //TODO
        } else if (ca.get_data_type() == ColumnAttribute.DataTyep::TEXT) {
            u16 size = *(u16) (bytles + offset);
            offset += sizeof(u16);
            //TODO
            offset += size;
        } else {
            throw DbRelationError("Cannot unmarshal ")
        }
    }
}
};


//TODO
//bool test_heap_storage();

