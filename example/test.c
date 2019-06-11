#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "h5_interface_bd.h"

#define FILE "../../bd-rdf/bd-rdf.h5"
#define FILE2 "dummy.h5"

int main(void){
	bd_file_t h5f = bd_open(FILE,NULL,false);
	char **groups = NULL;
	size_t num_groups = 0;
	bd_get_groups(h5f,NULL,&groups,&num_groups);
	bd_group_log_t g0 = bd_open_group_log(h5f,groups[0]);
	printf("opened group %s for reading.\ngroup.dim = %d\n",g0->name,g0->attributes.dimension);
	bd_table_frames_t tb = bd_open_table_frames(g0,"frames");
	if( tb ){
		printf("opened table frames.\n");
	}
	bd_table_frames_recordset_t r0 = NULL;
	size_t num_records = 2;
	bd_get_records_frames(tb, 0, &r0, &num_records);
	printf("r0->r[0] = (%f,%f,%f)\n",r0->set[0].r[0],r0->set[0].r[1],r0->set[0].r[2]);
	bd_close_table_frames_recordset(r0);
	bd_close_table_frames(tb);
	bd_close_group_log(g0);
	bd_close(h5f);
	for(int i=0;i<num_groups;++i){
		free(groups[i]);
	}
	free(groups);

	//create new file
	h5f = bd_create(FILE2);
	if( h5f ){
		printf("new file created.\n");
	}else{
		return 1;
	}
	bd_group_log_attributes attrs = {3,2,1.0,1.0,1e-5,1.0,10.0,0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL};
	g0 = bd_create_group_log(h5f, attrs, "flubber");
	if( g0 ){
		printf("group created.\n");
	}else{
		return 1;
	}

	tb = bd_create_table_frames(g0,"heinz");
	{
		unsigned int frame = 0;
		double time = 0.0;
		double v[6] = {1.,0.,0.,0.,1.,0.};
		double r[6] = {1.,1.,1.,2.,2.,2.};
		bd_table_frames_record rec0 = {&frame,&time,v,r};
		bd_add_records_frames(tb,1,&rec0);
		bd_add_records_frames(tb,1,&rec0);
		bd_add_records_frames(tb,1,&rec0);
	}
	bd_close_group_log(g0);
	bd_flush(h5f);
	bd_close(h5f);
	return 0;
}
