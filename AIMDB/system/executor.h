/**
 * @file    executor.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */

#ifndef _EXECUTOR_H
#define _EXECUTOR_H

#include "catalog.h"
#include "mymemory.h"
#include <algorithm>
#include <set>

#include <stdint.h>


#include <iostream>
#include <stddef.h>

// list node to link the *real* node into list
struct list_head {
	struct list_head *next, *prev;
};

#define typeof(x) __typeof__(x)

// check whether the list is empty (contains only one pseudo list node)
#define list_empty(list) ((list)->next == (list))

// get the *real* node from the list node
#define list_entry(ptr, type, member) \
		(type *)((char *)ptr - offsetof(type, member))

// iterate the list 
#define list_for_each_entry(pos, head, member) \
    for (pos = list_entry((head)->next, typeof(*pos), member); \
        	&pos->member != (head); \
        	pos = list_entry(pos->member.next, typeof(*pos), member)) 

// iterate the list safely, during which node could be added or removed in the list
#define list_for_each_entry_safe(pos, q, head, member) \
    for (pos = list_entry((head)->next, typeof(*pos), member), \
	        q = list_entry(pos->member.next, typeof(*pos), member); \
	        &pos->member != (head); \
	        pos = q, q = list_entry(pos->member.next, typeof(*q), member))

// initialize the list head
static inline void init_list_head(struct list_head *list)
{
	list->next = list->prev = list;
}

// insert a new node between prev and next
static inline void list_insert(struct list_head *_new,
			      struct list_head *prev,
			      struct list_head *next)
{
	next->prev = _new;
	prev->next = _new;
	_new->next = next;
	_new->prev = prev;
}

// add a list node at the head of the list
static inline void list_add_head(struct list_head *_new, struct list_head *head)
{
	list_insert(_new, head, head->next);
}

// add a list node at the tail of the list 
static inline void list_add_tail(struct list_head *_new, struct list_head *head)
{
	list_insert(_new, head->prev, head);
}

// delete the node from the list (note that it only remove the entry from 
// list, but not free allocated memory)
static inline void list_delete_entry(struct list_head *entry)
{
	entry->next->prev = entry->prev;
	entry->prev->next = entry->next;
}


typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

#define HASH_8BITS 256
#define HASH_16BITS 65536

// the simplest hash functions, you can recreate the wheels as you wish

static inline u8 hash8(char *buf, int len)
{
	u8 result = 0;
	for (int i = 0; i < len; i++)
		result ^= buf[i];

	return result;
}

static inline u16 hash16(char *buf, int len)
{
	u16 result = 0;
	for (int i = 0; i < len / 2 * 2; i += 2)
		result ^= *(u16 *)(buf + i);

	if (len % 2)
		result ^= (u8)(buf[len-1]);
	
	return result;
}


/** aggrerate method. */
enum AggregateMethod {
    NONE_AM = 0, /**< none */
    COUNT,       /**< count of rows */
    SUM,         /**< sum of data */
    AVG,         /**< average of data */
    MAX,         /**< maximum of data */
    MIN,         /**< minimum of data */
    MAX_AM
};

/** compare method. */
enum CompareMethod {
    NONE_CM = 0,
    LT,        /**< less than */
    LE,        /**< less than or equal to */
    EQ,        /**< equal to */
    NE,        /**< not equal than */
    GT,        /**< greater than */
    GE,        /**< greater than or equal to */
    LINK,      /**< join */
    MAX_CM
};

/** definition of request column. */
struct RequestColumn {
    char name[128];    /**< name of column */
    AggregateMethod aggrerate_method;  /** aggrerate method, could be NONE_AM  */
};

/** definition of request table. */
struct RequestTable {
    char name[128];    /** name of table */
};

/** definition of compare condition. */
struct Condition {
    RequestColumn column;   /**< which column */
    CompareMethod compare;  /**< which method */
    char value[128];        /**< the value to compare with, if compare==LINK,value is another column's name; else it's the column's value*/
};

/** definition of conditions. */
struct Conditions {
    int condition_num;      /**< number of condition in use */
    Condition condition[4]; /**< support maximum 4 & conditions */
};

/** definition of selectquery.  */
class SelectQuery {
  public:
    int64_t database_id;           /**< database to execute */
    int select_number;             /**< number of column to select */
    RequestColumn select_column[4];/**< columns to select, maximum 4 */
    int from_number;               /**< number of tables to select from */
    RequestTable from_table[4];    /**< tables to select from, maximum 4 */
    Conditions where;              /**< where meets conditions, maximum 4 & conditions */
    int groupby_number;            /**< number of columns to groupby */
    RequestColumn groupby[4];      /**< columns to groupby */
    Conditions having;             /**< groupby conditions */
    int orderby_number;            /**< number of columns to orderby */
    RequestColumn orderby[4];      /**< columns to orderby */
};  // class SelectQuery

/** definition of result table.  */
class ResultTable {
  public:
    int column_number;       /**< columns number that a result row consist of */
    BasicType **column_type; /**< each column data type */
    char *buffer;         /**< pointer of buffer alloced from g_memory */
    int64_t buffer_size;  /**< size of buffer, power of 2 */
    int row_length;       /**< length per result row */
    int row_number;       /**< current usage of rows */
    int row_capacity;     /**< maximum capacity of rows according to buffer size and length of row */
    int *offset;
    int offset_size;

    /**
     * init alloc memory and set initial value
     * @col_types array of column type pointers
     * @col_num   number of columns in this ResultTable
     * @param  capacity buffer_size, power of 2
     * @retval >0  success
     * @retval <=0  failure
     */
    int init(BasicType *col_types[],int col_num,int64_t capacity = 1024);
    /**
     * calculate the char pointer of data spcified by row and column id
     * you should set up column_type,then call init function
     * @param row    row id in result table
     * @param column column id in result table
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    char* getRC(int row, int column);
    /**
     * write data to position row,column
     * @param row    row id in result table
     * @param column column id in result table
     * @data data pointer of a column
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    int writeRC(int row, int column, void *data);
    /**
     * print result table, split by '\t', output a line per row 
     * @retval the number of rows printed
     */
    int print(void);
    /**
     * write to file with FILE *fp
     */
    int dump(FILE *fp);
    /**
     * free memory of this result table to g_memory
     */
    int shut(void);
};  // class ResultTable

/** definition of class executor.  */
class Executor {
  private:
    SelectQuery *current_query;  /**< selectquery to iterately execute */
  public:
    /**
     * exec function.
     * @param  query to execute, if NULL, execute query at last time 
     * @param result table generated by an execution, store result in pattern defined by the result table
     * @retval >0  number of result rows stored in result
     * @retval <=0 no more result
     */
    virtual int exec(SelectQuery *query, ResultTable *result);
    //--------------------------------------
    //  ... 
    //  ...
    /**
     * close function.
     * @param None
     * @retval ==0 succeed to close
     * @retval !=0 fail to close
     */
    virtual int close();
};

#define HASH_SIZE 256

int64_t getTid(int64_t c_id, RequestTable *from_table, int from_number);

class Scan;
class Filter;
class Join;
class Project;

struct hash_row{
	struct list_head list;		/**< list head for hash table */
	int64_t record_rank;		/**< record rank in hashed table */
	int cnt;					/**< counter of rows, used in AVG */
};

/** definition of class myhashtable. */
class MyHashTable {
	public:
		struct list_head hash_row_table[HASH_SIZE];		/**< hash table list */
		int64_t t_id;									/**< hashed table id */
		int64_t c_id;									/**< hashed column id */
		BasicType *t;									/**< hashed column type */
		int64_t offset;									/**< offset of hashed column in table */
		Filter *filter;									/**< pointer of filter class */
		int64_t cur_record_rank;						/**< current record rank in loop */

		/**
			 * myhashtable constructor function.
			 * @param t_id hash table id 
			 * @param c_id hash column id
			 * @param where filter condition, used to construct filter
			 */
		MyHashTable(int64_t t_id, int64_t c_id, Conditions *where);

		/**
			 * look up hash table with data
			 * @param data pointer to data, its type is this->t
			 * @retval >= 0 column rank
			 * @retval >= -1 not found
			 */
		int64_t lookup(char* data);

};

/** definition of hash for group by. */
class HashforGroupby {
	public:
		struct list_head hash_table[HASH_SIZE];		/**< hash table list */
		int groupby_number;							/**< number of group by items */
		int cols[4];								/**< column rank in result table of group by entries */
		int64_t off[4];								/**< offset in result table of group by entries */
		AggregateMethod aggMthd[4];					/**< aggregate method of columns in result table */
		BasicType * t[4];							/**< type of columns in result table */
		ResultTable * result;						/**< pointer of result table */
		/**
			 * HashforGroupby function
			 * @param result pointer of result table
			 * @param query pointer of select query
			 */
		HashforGroupby(ResultTable * result, SelectQuery * query);
		/**
			 * get hash key from row in result table
			 * @param data row in result table
			 * @retval 8-bit hash key
			 */
		u8 getHashKey(char * data);
		/**
			 * add a row in result table into hash table
			 * @param row row rank in result table 
			 */
		void hash(int row);
		/**
			 * update aggregation data in result table
			 * @param data data of a new row
			 * @param hr pointer of hash row
			 */
		void update(char * data, struct hash_row * hr);
		/**
			 * look up hash table with data
			 * @param data data of a row
			 * @retval !=NULL pointer of hash row
			 * @retval ==NULL not found
			 */
		struct hash_row * lookup(char * data);
		/**
			 * handle AVG aggregation after buinding result table
			 */
		void handleAVG();
};

/** definition of having. */
class Having {
	public:
		int having_number;			/**< number of having entries */
		int64_t cols[4];			/**< column rank in result table of habing entries */
		int64_t off[4];				/**< offset in result table of having entries */
		CompareMethod method[4];	/**< compare method of having entries */
		BasicType * t[4];			/**< type of columns in result table */
		char * val[4];				/**< compare values of having entries */
		/**
			 * having function
			 * @param res pointer of result table
			 * @param query pointer of select query
			 */
		Having(ResultTable * res, SelectQuery * query);
		/**
			 * judge a row in result table by having conditions
			 * @param data data of a row in result table
			 * @retval true succcess
			 * @retval false failure
			 */
		bool judge(char * data);
};

/** definition of orderby. */
struct Orderby{
	int orderby_number;		/**< number of order by entries */
	int64_t off[4];			/**< offset in result table of order by entries */
	BasicType * t[4];		/**< type of order by entries */
};

/**
	 * init order by struct
	 * @param res pointer of result table
	 * @param query pointer of select query
	 * @retval pointer of order by struct
	 */
struct Orderby * initOrderby(ResultTable * res, SelectQuery * query);
/**
	 * compare function based on order by struct, used in qsort
	 * @param data1 first data to compare
	 * @param data2 second data to compare
	 * @retval >0	data1>data2
	 * @retval ==0	data1==data2
	 * @retval <0	data1<data2
	 */
int Orderby_cmp(const void * data1, const void * data2);

/** definition of Operator. */
class Operator {
	public:
		virtual bool	reset    ();
		virtual	void*	getNext ();
		virtual	bool	isEnd   ();
};

/** definition of Project. */
class Project{
	public:
		Join *child;				/**< pointer of child node */
		int sel_num;				/**< number of selected columns */
		int64_t sel_t_id[4];		/**< selected table identifier */
		int64_t sel_c_id[4];		/**< selected column identifier */
		AggregateMethod sel_agg[4]; /**< select aggregation */

		/**
		 * Project constructor function.
		 * @param query pointer to a sql query
		 */
		Project(SelectQuery *query);

		/**
		 * get next row after projection
		 * @param buffer buffer to store the row
		 * @retval != NULL pointer to buffer
		 * @retval = NULL query finished
		 */
		void* getNext(char *buffer);

		/**
		 * destroy Project and its offspring 
		 */
		bool destroy();
};

/** definition of Join. */
class Join{
	public:
		Project *parent;		/**< parent pointer */
		Filter *child;			/**< child pointer */
		int from_number;		/**< number of tables the query invovles */
		int link_number;		/**< number of links, up to 3 */
		int64_t c_id[3][2];		/**< column identifier with links. If id1 and id2 is the first link, then c_id[0] = {id1, id2} */
		int64_t t_id[3][2];		/**< table identifier with links. Corresponding with c_id array, t_id is their table identifiers */
								/**< the sequence of c_id[4] is not necessarily the same with the sequence of links in where condition. */
		MyHashTable *hash_table[3];		/**< hash table array, up to 3 */
		int64_t i_id[3];		/**< index identifier, up to 3 */
		bool has_index[3];		/**< indicates whether the corresponding link has index */
		int64_t record_rank[4];		/**< record the current query status, record_rank[i] records the current record rank of
									 * i-th from table */
									/**< the sequence of from table is t_id[0][0], t_id[0][1], t_id[1][1], t_id[2][1], 
									 * if there are so many links */ 

		/**
		 * Project constructor function.
		 * @param parent pointer to parent
		 * @param from_number number of from table
		 * @param from_table pointer to from tables
		 * @param where condition of where, including links
		 */
		Join(Project *parent, int from_number, RequestTable *from_table, Conditions *where);

		/**
		 * get index of column
		 * @param c_id column identifier
		 * @retval != -1 index of the column
		 * @retval = -1 the column does not have index
		 */
		int64_t getIndex(int64_t c_id, RequestTable *from_table, int from_number)
		{
			int t_id;
			// int64_t t_id = ((Column*)(g_catalog.getObjById(c_id)))->getTid();
			t_id = getTid(c_id, from_table, from_number);
			RowTable* tp = (RowTable*)(g_catalog.getObjById(t_id));
			for(size_t i = 0; i < tp->getIndexs().size(); i++)
			{
				if( ((Index*)(g_catalog.getObjById((tp->getIndexs())[i])))->getIKey().contain(c_id) == true )
					return (tp->getIndexs())[i];
			}
			return -1;
		}

		/**
		 * Join get next line.
		 * @param query pointer to an array of int64_t. Every 2 elements of the array is a tuple, 
		 * 				represents t_id and record_rank respectively, up to 4 tuples. The last element
		 * 				of the array is set to -1
		 * @retval pointer to ret
		 */
		void* getNext(int64_t *ret);

		/**
		 * destroy Project and its offspring 
		 */
		bool destroy();
		
};

/** definition of filter. */
class Filter{
	public:
		void *parent;		/**< parent node, could be Join* or MyHashTable* */
		Scan *child;		/**< child node */
		int64_t t_id;		/**< table identifier */
		int filter_number;			/**< number of filter conditions in where clause */
		CompareMethod method[4];	/**< compare method */
		int64_t col_rank[4];		/**< which column is being compared */
		AggregateMethod am[4];		/**< Aggregate Method */
		BasicType *type[4];			/**< type of each column */
		char *val[4];				/**< values that are compared to */

		/**
		 * Filter constructor function.
		 * @param parent pointer to parent
		 * @param t_id table identifier
		 * @param where condition of where, including filter conditions
		 */
    	Filter(void *parent, int64_t t_id, Conditions *where);

		// 返回int64_t类型指针，指向record_rank
		/**
		 * Filter get next line
		 * @retval pointer to the next record_rank satisfying filter conditions
		 */
		void* getNext();

		/**
		 * judge if get the end of table
		 * @retval true is end
		 * @retval false not end
		 */
		bool isEnd();

		/**
		 * destroy Project and its offspring 
		 */
		bool destroy();
};

/** definition of scan. */
class Scan{
	public:
		Filter *parent;			/**< pointer of parent operator */
		int64_t t_id;			/**< id of table to scan */
		int64_t record_rank;	/**< current scanning record rank */
		int64_t record_num;		/**< total number of records in table */
		/**
			 * scan function
			 * @param parent pointer of parent operator
			 * @param t_id id of table to scan
			 */
		Scan(Filter *parent, int64_t t_id);
		/**
			 * reset record rank to beginning of table3
			 * @retval always true
			 */
		bool reset(){
			record_rank = -1;
			return true;
		}
		/**
			 * get next record rank in table
			 * @retval record rank
			 */
		void* getNext();
		/**
			 * judge if get the end of table
			 * @retval true is end
			 * @retval false not end
			 */
		bool isEnd();
		/**
			 * destroy the operator
			 * @retval true succcess
			 * @retval false failure
			 */
		bool destroy();
	
};

extern ResultTable r_table;
#endif
