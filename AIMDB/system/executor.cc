/**
 * @file    executor.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */
#include "executor.h"

ResultTable r_table;

struct Orderby * odr;

int64_t getTid(int64_t c_id, RequestTable *from_table, int from_number)
{
	for(int i = 0; i < from_number ; i++)
	{
		RowTable *op;
		if( (op = (RowTable*)(g_catalog.getObjByName(from_table[i].name))) == NULL )
			continue;
		for(int j = 0; j < op->getColumns().size(); j++)
		{
			if(op->getColumns()[j] == c_id)
				return op->getOid();
		}
	}
	return -1;
}

int Executor::exec(SelectQuery *query, ResultTable *result)
{
    //---------write your code here-------------------------
	if(query == NULL)
	{
		BasicType *col_types[1];
		col_types[0] = new TypeInt32();
		int capacity_power = 12;
		result->init(col_types, 1, 2 << capacity_power);
		return 0;
	}

	int capacity_power = 12;

	BasicType *col_types[query->select_number];
	for(int ii = 0; ii < query->select_number; ii++){
		if(query->select_column[ii].aggrerate_method == COUNT)
		{
			col_types[ii] = new TypeInt32();
		} else {
			Column * col = (Column *)g_catalog.getObjByName(query->select_column[ii].name);
			col_types[ii] = col->getDataType();
		}
	}
	resize:
	result->init(col_types, query->select_number, 2 << capacity_power);

	HashforGroupby * hash = new HashforGroupby(result, query);
	Having * have = new Having(result, query);
	odr = initOrderby(result, query);

	Project *project = new Project(query);

	char *off = result->buffer;
	char buffer[1024];
	char *ret;
	int row = 0;
	while((ret = (char*)(project->getNext(buffer))) != NULL){
		if(!have->judge(buffer))continue;
		//printf("%d ", row);
		if(query->groupby_number > 0){
			//do group by
			struct hash_row * hr = hash->lookup(buffer);
			if(hr != NULL){
				hash->update(buffer, hr);
			}
			else{
				int offset = 0;
				for(int i = 0; i < result->column_number; i++)
				{
					if(query->select_column[i].aggrerate_method == COUNT){
						int cnt = 1;
						result->writeRC(row, i, &cnt);
					}
					else result->writeRC(row, i, ret + offset);
					offset += result->column_type[i]->getTypeSize();
				}
				hash->hash(row);
				row++;
				result->row_number++;
				if(result->row_number >= result->row_capacity)
				{
					result->shut();
					capacity_power++;
					project->destroy();
					delete project;
					goto resize;
				}
			}
		}
		else{
			int offset = 0;
			for(int i = 0; i < result->column_number; i++)
			{
				result->writeRC(row, i, ret + offset);
				offset += result->column_type[i]->getTypeSize();
			}
			// memcpy(off, ret, result->row_length);
			// off += result->row_length;
			row++;
			result->row_number++;
			if(result->row_number >= result->row_capacity)
			{
				result->shut();
				capacity_power++;
				project->destroy();
				delete project;
				goto resize;
			}
		}
	}

	ret:

	if(query->groupby_number > 0)hash->handleAVG();

	if(odr->orderby_number > 0) qsort(result->buffer, row, result->row_length, Orderby_cmp);

	return row;
	
}

int Executor::close() 
{
    //---------write your code here-------------------------
    
    return 0;
}

// note: you should guarantee that col_types is useable as long as this ResultTable in use, maybe you can new from operate memory, the best method is to use g_memory.
int ResultTable::init(BasicType *col_types[], int col_num, int64_t capacity) {
	column_type = (BasicType**)malloc(sizeof(BasicType*) * col_num);
	for(int i =0; i < col_num; i++)
		column_type[i] = col_types[i];
    column_number = col_num;
    row_length = 0;
    buffer_size = g_memory.alloc (buffer, capacity);
    if(buffer_size != capacity) {
        printf ("[ResultTable][ERROR][init]: buffer allocate error!\n");
        return -1;
    }
    int allocate_size = 1;
    int require_size = sizeof(int64_t)*column_number;
    while (allocate_size < require_size)
        allocate_size = allocate_size << 1;
    char *p = NULL;
    offset_size = g_memory.alloc(p, allocate_size);
    if (offset_size != allocate_size) {
        printf ("[ResultTable][ERROR][init]: offset allocate error!\n");
        return -2;
    }
    offset = (int*) p;
    for(int ii = 0;ii < column_number;ii ++) {
        offset[ii] = row_length;
        row_length += column_type[ii]->getTypeSize(); 
    }
    row_capacity = (int)(capacity / row_length);
    row_number   = 0;
    return 0;
}

int ResultTable::print (void) {
    int row = 0;
    int ii = 0;
    char buffer[1024];
    char *p = NULL; 
    while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = getRC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            printf("%s\t", buffer);
        }
        p = getRC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        printf("%s\n", buffer);
        row++; 
		ii=0;
    }
    return row;
}

int ResultTable::dump(FILE *fp) {
    // write to file
    int row = 0;
    int ii = 0;
    char buffer[1024];
    char *p = NULL; 
    while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = getRC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            fprintf(fp,"%s\t", buffer);
        }
        p = getRC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        fprintf(fp,"%s\n", buffer);
        row ++; ii=0;
    }
    return row;
}

// this include checks, may decrease its speed
char* ResultTable::getRC(int row, int column) {
    return buffer+ row*row_length+ offset[column];
}

int ResultTable::writeRC(int row, int column, void *data) {
    char *p = getRC (row,column);
    if (p==NULL) return 0;
    return column_type[column]->copy(p,data);
}

int ResultTable::shut (void) {
    // free memory
    g_memory.free (buffer, buffer_size);
    g_memory.free ((char*)offset, offset_size);
	free(column_type);
    return 0;
}

//---------------------operators implementation---------------------------
// ...
// ...
MyHashTable::MyHashTable(int64_t t_id, int64_t c_id, Conditions *where) {
	for (int i = 0; i < HASH_SIZE; i++)
		init_list_head(&hash_row_table[i]);
	this->t_id = t_id;
	this->c_id = c_id;
	filter = new Filter(this, t_id, where);
	// establish hash table by t_id and c_id
	RowTable* tp = (RowTable*)(g_catalog.getObjById(t_id));
	int64_t col_rank = tp->getColumnRank(c_id);
	this->t = tp->getRPattern().getColumnType(col_rank);
	int64_t record_num = tp->getMStorage().getRecordNum();
	int64_t record_rank = 0;
	this->offset = tp->getRPattern().getColumnOffset(col_rank);
	while(1)
	{
		void *p = NULL;
		if((p = filter->getNext()) == NULL)
			break;
		record_rank = *((int64_t*)p);
		char *rp = tp->getMStorage().getRow(record_rank);
		int hash_key = hash8(rp+offset, t->getTypeSize());
		hash_row *hr = (hash_row*)malloc(sizeof(hash_row));
		init_list_head(&(hr->list));
		hr->record_rank = record_rank;
		list_add_tail(&(hr->list), &(hash_row_table[hash_key]));
	}
	cur_record_rank = -1;
}

int64_t MyHashTable::lookup(char* data) {
	int hash_key = hash8(data, t->getTypeSize());
	RowTable* tp = (RowTable*)(g_catalog.getObjById(t_id));
	hash_row *hr = NULL;
	int flag = 0;
	list_for_each_entry(hr, &hash_row_table[hash_key], list)
	{
		char *rp = tp->getMStorage().getRow(hr->record_rank);
		if(t->cmpEQ(rp+offset, data) == true)
		{
			if(cur_record_rank == -1 || flag == 1)
				return cur_record_rank = hr->record_rank;
			if(hr->record_rank == cur_record_rank)
				flag = 1;
		}
	}
	return cur_record_rank = -1;
}


HashforGroupby::HashforGroupby(ResultTable * res, SelectQuery * query){
	if(query->groupby_number <= 0) return;
	for (int i = 0; i < HASH_SIZE; i++)
		init_list_head(&hash_table[i]);
	for(int i = 0; i < query->select_number; i++){
		t[i] = res->column_type[i];
		aggMthd[i] = query->select_column[i].aggrerate_method;
	}
	for(int i = query->select_number; i < 4; i++){
		t[i] = NULL;
		aggMthd[i] = NONE_AM;
	}
	result = res;
	groupby_number = query->groupby_number;
	printf("%d\n", groupby_number);
	for(int i = 0; i < groupby_number; i++){
		for(int j = 0; j < query->select_number; j++){
			if(memcmp(query->groupby[i].name, query->select_column[j].name, strlen(query->groupby[i].name)) == 0){
				cols[i] = j;
				off[i] = result->offset[j];
				break;
			}
		}
	}
}

u8 HashforGroupby::getHashKey(char * data){
	u8 hash_key = 0;
	for(int i = 0; i < groupby_number; i++){
		hash_key += hash8(data + off[i], t[cols[i]]->getTypeSize());
	}
	return hash_key;
}

void HashforGroupby::hash(int row){
	u8 key = getHashKey(result->buffer + row*result->row_length);
	struct hash_row * hr = (hash_row *)malloc(sizeof(struct hash_row));
	hr->record_rank = row;
	hr->cnt = 1;
	init_list_head(&hr->list);
	list_add_head(&hr->list, &hash_table[key]);
}

void HashforGroupby::update(char * data, struct hash_row * hr){
	int off = 0;
	int row = hr->record_rank;
	hr->cnt++;
	char * buff = result->buffer + row*result->row_length;
	for(int i = 0; t[i] != NULL; i++) {
		if(aggMthd[i] == COUNT) {
			*(int32_t *)(buff + off) += 1;
		}
		else if(aggMthd[i] == MAX) {
			if(t[i]->cmpGT(data + off, buff + off)){
				memcpy(buff + off, data + off, t[i]->getTypeSize());
			}
		}
		else if(aggMthd[i] == MIN) {
			if(t[i]->cmpLT(data + off, buff + off)) {
				memcpy(buff + off, data + off, t[i]->getTypeSize());
			}
		}
		else if(aggMthd[i] == SUM || aggMthd[i] == AVG) {
			TypeCode type = t[i]->getTypeCode();
			if(type == INT8_TC){
				*(int8_t *)(buff + off) += *(int8_t *)(data + off);
			}
			else if(type == INT16_TC){
				*(int16_t *)(buff + off) += *(int16_t *)(data + off);
			}
			else if(type == INT32_TC){
				*(int32_t *)(buff + off) += *(int32_t *)(data + off);
			}
			else if(type == INT64_TC){
				*(int64_t *)(buff + off) += *(int64_t *)(data + off);
			}
			else if(type == FLOAT32_TC){
				*(float *)(buff + off) += *(float *)(data + off);
			}
			else if(type == FLOAT64_TC){
				*(double *)(buff + off) += *(double *)(data + off);
			}
		}
		off+=t[i]->getTypeSize();
	}
}

struct hash_row * HashforGroupby::lookup(char * data) {
	u8 hash_key = getHashKey(data);
	//RowTable* tp = (RowTable*)(g_catalog.getObjById(t_id));
	hash_row *hr = NULL;
	list_for_each_entry(hr, &hash_table[hash_key], list)
	{
		int flag = 0;
		for(int i = 0; i < groupby_number; i++){
			char * hashed_data = result->getRC(hr->record_rank, cols[i]);
			if(t[cols[i]]->cmpEQ(hashed_data, data + off[i])) flag = 1;
			else{
				flag = 0;
				break;
			}
		}
		if(flag == 1) return hr;
	}
	return NULL;
}

void HashforGroupby::handleAVG(){
	int off = 0;
	for(int i = 0; i < 4; i++){
		if(t[i] == NULL)break;
		if(aggMthd[i] == AVG){
			for(int j = 0; j < HASH_SIZE; j++){
				struct hash_row * hr = NULL;
				list_for_each_entry(hr, &hash_table[j], list){
					//printf("***%d %d\n", hr->record_rank, hr->cnt);
					char * buff = result->buffer + hr->record_rank * result->row_length;
					TypeCode type = t[i]->getTypeCode();
					if(type == INT8_TC){
						*(int8_t *)(buff + off) /= hr->cnt;
					}
					else if(type == INT16_TC){
						*(int16_t *)(buff + off) /= hr->cnt;
					}
					else if(type == INT32_TC){
						*(int32_t *)(buff + off) /= hr->cnt;
					}
					else if(type == INT64_TC){
						*(int64_t *)(buff + off) /= hr->cnt;
					}
					else if(type == FLOAT32_TC){
						*(float *)(buff + off) /= (float)hr->cnt;
					}
					else if(type == FLOAT64_TC){
						*(double *)(buff + off) /= (double)hr->cnt;
					}
					
				}
			}
		}
		off += t[i]->getTypeSize();
	}
}


Having::Having(ResultTable * res, SelectQuery * query){
	having_number = query->having.condition_num;
	for(int i = 0; i < query->select_number; i++){
		t[i] = res->column_type[i];
	}
	for(int i = query->select_number; i < 4; i++){
		t[i] = NULL;
	}
	for(int i = 0; i < having_number; i++){
		method[i] = query->having.condition[i].compare;
		for(int j = 0; j < query->select_number; j++){
			if(memcmp(query->having.condition[j].column.name, query->select_column[j].name, strlen(query->select_column[j].name)) == 0){
				cols[i] = j;
				val[i] = (char *)malloc(t[j]->getTypeSize());
				t[j]->formatBin(val[i], query->having.condition[i].value);
				off[i] = res->offset[j];
				break;
			}
		}
	}
}

bool Having::judge(char * data){
	for(int i = 0; i < having_number; i++){
		switch (method[i]) {
		case LT:
			if(t[i]->cmpLT(data + off[i], val[i]) == false)return false;
			break;
		case LE:
			if(t[i]->cmpLE(data + off[i], val[i]) == false)return false;
			break;
		case EQ:
			if(t[i]->cmpEQ(data + off[i], val[i]) == false)return false;
			break;
		case NE:
			if(t[i]->cmpEQ(data + off[i], val[i]) == true)return false;
			break;
		case GT:
			if(t[i]->cmpGT(data + off[i], val[i]) == false)return false;
			break;
		case GE:
			if(t[i]->cmpGE(data + off[i], val[i]) == false)return false;
			break;
		default:
			break;
		}
	}
	return true;
}


struct Orderby * initOrderby(ResultTable * res, SelectQuery * query){
	struct Orderby * odr = (struct Orderby *)malloc(sizeof(struct Orderby));
	odr->orderby_number = query->orderby_number;
	for(int i = 0; i < odr->orderby_number; i++){
		for(int j = 0; j < query->select_number; j++){
			if(memcmp(query->orderby[i].name, query->select_column[j].name, strlen(query->select_column[j].name)) == 0){
				odr->t[i] = res->column_type[j];
				//cols[i] = j;
				odr->off[i] = res->offset[j];
				break;
			}
		}
	}
	return odr;
}

int Orderby_cmp(const void * data1, const void * data2){
	for(int i = 0; i < odr->orderby_number; i++){
		if(odr->t[i]->cmpLT((char*)data1 + odr->off[i], (char*)data2 + odr->off[i])) return -1;
		if(odr->t[i]->cmpGT((char*)data1 + odr->off[i], (char*)data2 + odr->off[i])) return 1;
	}
	return 0;
}


Project::Project(SelectQuery *query) {
	sel_num = query->select_number;
	for(int i = 0; i < sel_num; i++)
	{
		sel_c_id[i] = ((Column*)(g_catalog.getObjByName(query->select_column[i].name)))->getOid();
		sel_t_id[i] = getTid(sel_c_id[i], query->from_table, query->from_number);
		sel_agg[i] = query->select_column[i].aggrerate_method;
	}
	child = new Join(this, query->from_number, query->from_table, &(query->where));
}

void* Project::getNext(char *buffer){
	int64_t next[9];
	this->child->getNext(next);		// 数组每相邻两个元素为一组，分别代表t_id和record_rank，最多4组
	if(next[0] == -1)
		return NULL;
	char *rp = NULL;
	char *new_rp;
	int offset = 0;

	for(int j = 0; j < sel_num; j++)
	{
		if(sel_agg[j] == COUNT){
			int cnt = 1;
			memcpy(buffer + offset, &cnt, 4);
			offset+=4;
			continue;
		}
		for(int i = 0; i < 8; i +=2)
		{
			if(next[i] == -1)
				break;
			int64_t t_id = next[i];
			int64_t record_rank = next[i+1];
			if(sel_t_id[j] == t_id)		// 投影列所在的表和子节点传过来的表一致
			{
				RowTable *rt = ((RowTable*)(g_catalog.getObjById(t_id)));
				rp = rt->getMStorage().getRow(record_rank);
				int64_t col_rank = rt->getColumnRank(sel_c_id[j]);
				BasicType *t = rt->getRPattern().getColumnType(col_rank);
				int r_offset = rt->getRPattern().getColumnOffset(col_rank);
				t->copy(buffer+offset, rp+r_offset);
				offset += t->getTypeSize();
			}
		}
	}

	buffer[offset] = 0;
	return buffer;
}

bool Project::destroy(){
	child->destroy();
	delete child;
	return true;
}


Join::Join(Project *parent, int from_number, RequestTable *from_table, Conditions *where){
	this->parent = parent;
	this->from_number = from_number;
	this->link_number = 0;
	this->record_rank[0] = -1;
	int64_t c_id[4][2];
	int64_t t_id[4][2];
	for(int i = 0; i < where->condition_num; i++)
	{
		if(where->condition[i].compare != LINK)
			continue;
		int c_id_0, c_id_1, t_id_0, t_id_1;
		RowTable *tp0, *tp1;
		c_id_0 = (g_catalog.getObjByName(where->condition[i].column.name))->getOid();
		c_id_1 = (g_catalog.getObjByName(where->condition[i].value))->getOid();
		t_id_0 = getTid(c_id_0, from_table, from_number); 
		t_id_1 = getTid(c_id_1, from_table, from_number);
		tp0 = (RowTable*)(g_catalog.getObjById(t_id_0));
		tp1 = (RowTable*)(g_catalog.getObjById(t_id_1));

		if(getIndex(c_id_0, from_table, from_number) != -1)		
			goto assign1;
		else if(getIndex(c_id_1, from_table, from_number) != -1)
			goto assign0;
		else {		// no index, use simple hash join
			// let the size of table corresponding to t_id[][0] smaller than the size of table corresponding to t_id[][1]
			if(tp0->getMStorage().getRecordNum() < tp1->getMStorage().getRecordNum())
				goto assign1;
			else goto assign0;
		}
		assign0:
			c_id[link_number][0] = c_id_0;
			c_id[link_number][1] = c_id_1;
			t_id[link_number][0] = t_id_0;
			t_id[link_number][1] = t_id_1;
			goto assign_finish;
		assign1:
			c_id[link_number][0] = c_id_1;
			c_id[link_number][1] = c_id_0;
			t_id[link_number][0] = t_id_1;
			t_id[link_number][1] = t_id_0;
		assign_finish:
			link_number++;
	}
	int seq_set[4] = {0,0,0,0};
	for(int i = 0; i < link_number; i++)
	{
		for(int j = 0; j < link_number; j++)
		{
			if(seq_set[j])
				continue;
			int sel = 1;
			for(int k = 0; k < link_number; k++)
			{
				if(seq_set[k] || j == k)
					continue;
				if(t_id[j][0] == t_id[k][1])
				{
					sel = 0;
					break;
				}
			}
			if(sel)
			{
				this->c_id[i][0] = c_id[j][0];
				this->c_id[i][1] = c_id[j][1];
				this->t_id[i][0] = t_id[j][0];
				this->t_id[i][1] = t_id[j][1];
				seq_set[j] = 1;
				break;
			}
		}
	}
	for(int i = 0; i < link_number; i++)
	{
		int index;
		if((index = getIndex(this->c_id[i][0], from_table, from_number)) != -1)
		{
			has_index[i] = true;
			i_id[i] = index;
		}
		else
		{
			has_index[i] = false;
			hash_table[i] = new MyHashTable(this->t_id[i][1], this->c_id[i][1], where);
		}
	}
	if(link_number == 0)
		this->t_id[0][0] = ((RowTable*)(g_catalog.getObjByName(from_table[0].name)))->getOid();
	child = new Filter(this, this->t_id[0][0], where);
}

void* Join::getNext(int64_t *ret){
	if( from_number == 1 )		// no join
	{
		int64_t *p;
		if((p = (int64_t*)child->getNext()) == NULL)
		{
			ret[0] = -1;
			return ret;
		}
		ret[0] = t_id[0][0];
		ret[1] = *p;
		ret[2] = -1;
		return ret;
	}
	RowTable* tp;
	int64_t col_rank;
	char* rp;
	int offset, l;

	if(record_rank[0] == -1)
		goto next;


	if(link_number == 1)
		goto hash0;
	else if(link_number == 2)
		goto hash1;
	else if(link_number == 3)
		goto hash2;

	hash2:
	tp = (RowTable*)(g_catalog.getObjById(t_id[2][0]));
	if(t_id[2][0] == t_id[0][0]) l = 0;
	else if(t_id[2][0] == t_id[0][1]) l = 1;
	else if(t_id[2][0] == t_id[1][1]) l = 2;
	else {
		printf("ERROR: t_id not found\n");
		exit(1);
	}
	rp = tp->getMStorage().getRow(record_rank[l]);
	col_rank = tp->getColumnRank(c_id[2][0]);
	offset = tp->getRPattern().getColumnOffset(col_rank);
	if( (record_rank[3] = hash_table[2]->lookup(rp+offset)) == -1 )
		goto hash1;
	else goto ret;
	hash1:
	tp = (RowTable*)(g_catalog.getObjById(t_id[1][0]));
	if(t_id[1][0] == t_id[0][0]) l = 0;
	else if(t_id[1][0] == t_id[0][1]) l = 1;
	else {
		printf("ERROR: t_id not found\n");
		exit(1);
	}
	rp = tp->getMStorage().getRow(record_rank[l]);
	col_rank = tp->getColumnRank(c_id[1][0]);
	offset = tp->getRPattern().getColumnOffset(col_rank);
	if( (record_rank[2] = hash_table[1]->lookup(rp+offset)) == -1 )
		goto hash0;
	else if(link_number == 3) goto hash2;
	else goto ret;
	hash0:
	tp = (RowTable*)(g_catalog.getObjById(t_id[0][0]));
	rp = tp->getMStorage().getRow(record_rank[0]);
	col_rank = tp->getColumnRank(c_id[0][0]);
	offset = tp->getRPattern().getColumnOffset(col_rank);
	if( (record_rank[1] = hash_table[0]->lookup(rp+offset)) == -1 )
		goto next;
	else if(link_number >= 2) goto hash1;
	else goto ret;
	next:
	int64_t *p;
	if( (p = (int64_t*)(child->getNext())) == NULL )
	{
		ret[0] = -1;
		return NULL;
	}
	record_rank[0] = *p;
	goto hash0;
	
	ret:
	for(int i = 0; i <= link_number; i++)
	{
		if(i == 0)
			ret[2*i] = t_id[i][0];
		else ret[2*i] = t_id[i-1][1];
		ret[2*i+1] = record_rank[i];
	}
	ret[2*link_number+2] = -1;
	return ret;
}

bool Join::destroy(){
	child->destroy();
	delete child;
	for(int i = 0; i < link_number; i++)
		delete hash_table[i];
	return true;
}


Filter::Filter(void *parent, int64_t t_id, Conditions *where){
	this->parent = parent;
	this->t_id = t_id;
	RowTable *t = (RowTable*)(g_catalog.getObjById(t_id));
	filter_number = 0;
	for(int i = 0; i < where->condition_num; i++)
	{
		if(where->condition[i].compare == LINK)
			continue;
		Column *cp = (Column*)(g_catalog.getObjByName(where->condition[i].column.name));
		RowTable* tp = (RowTable*)(g_catalog.getObjById(t_id));
		int found = 0;
		for(int j = 0; j < tp->getColumns().size(); j++)
		{
			if(tp->getColumns()[j] == cp->getOid())
			{
				found = 1;
				break;
			}
		}
		if(found == 0)
			continue;
		// now we find true filter condition for this table
		method[filter_number] = where->condition[i].compare;
		col_rank[filter_number] = t->getColumnRank(cp->getOid());
		am[filter_number] = where->condition[i].column.aggrerate_method;
		type[filter_number] = t->getRPattern().getColumnType(col_rank[filter_number]);
		val[filter_number] = (char*)malloc(type[filter_number]->getTypeSize());
		type[filter_number]->formatBin(val[filter_number], where->condition[i].value);
		filter_number++;
	}
	child = new Scan(this, t_id);
}

void* Filter::getNext(){
	while(1)
	{
		int64_t *next = (int64_t *)(this->child->getNext());
		if(next == NULL)
			return NULL;
		int64_t *record_rank = next;
		RowTable *tp = (RowTable *)(g_catalog.getObjById(t_id));
		char* rp = tp->getMStorage().getRow(*record_rank);
		for(int i = 0; i < filter_number; i++)
		{
			char *data = rp + tp->getRPattern().getColumnOffset(col_rank[i]);
			switch (method[i]) {
			case LT:
				if(type[i]->cmpLT(data, val[i]) == false)
					goto next;
				break;
			case LE:
				if(type[i]->cmpLE(data, val[i]) == false)
					goto next;
				break;
			case EQ:
				if(type[i]->cmpEQ(data, val[i]) == false)
					goto next;
				break;
			case NE:
				if(type[i]->cmpEQ(data, val[i]) == true)
					goto next;
				break;
			case GT:
				if(type[i]->cmpGT(data, val[i]) == false)
					goto next;
				break;
			case GE:
				if(type[i]->cmpGE(data, val[i]) == false)
					goto next;
				break;
			default:
				break;
			}
		}
		return record_rank;
		next:
			;
	}
}

bool Filter::destroy(){
	child->destroy();
	delete child;
	return true;
}


Scan::Scan(Filter *parent, int64_t t_id) {
	this->parent = parent;
	this->t_id = t_id;
	record_rank = -1;
	record_num = ((RowTable*)(g_catalog.getObjById(t_id)))->getRecordNum();
}

void* Scan::getNext(){		
	if( this->isEnd() == true )
		return NULL;
	record_rank++;
	return &record_rank;
}

bool Scan::isEnd(){
	if( record_rank >= record_num - 1 )
		return true;
	return false;
}

bool Scan::destroy(){
	return true;
}

/* 

set args ../Lab3_Test1/data/tpch_schema_small_r.txt ../Lab3_Test1/data/sf10Mt/
b Join
b Scan
b Filter
b Project 
b MyHashTable
b Join::getNext(long*)
b Scan::getNext()
b Filter::getNext()
b Project::getNext(char*)
b MyHashTable::lookup(char*) 

*/