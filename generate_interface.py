#!/usr/bin/env python3
import yaml
import random
import string
import sys

#generic config
h5_types = {"int":"H5T_NATIVE_INT",
            "unsigned int":"H5T_NATIVE_UINT",
            "char":"H5T_NATIVE_CHAR",
            "double":"H5T_NATIVE_DOUBLE"}

h5_att_types = {"int":"int",
                "unsigned int":"uint",
                "char":"char",
                "double":"double"}
#function
def rnd_name():
    return "".join(random.choices(string.ascii_lowercase,k=10))

def int_or_var(var,prefix=""):
    try:
        r = str(int(var))
    except ValueError:
        r = "({})".format(prefix+var)
    return r

#string literals
code_logfile_header = """#ifndef _HDF_HL_H
    #include <hdf5_hl.h>
#endif

typedef struct _{} {{
    hid_t root;
    hid_t h5_file;
    bool rw;
}} {name}_file;

typedef {name}_file * {name}_file_t;

"""

code_group_generics = """    char *name;
    hid_t h5_group;
    {name}_file_t parent;
    {name}_group_{gname}_attributes attributes;
"""

code_table_header = """typedef struct _{} {{
    size_t record_size;
    size_t *column_offsets;
    size_t *column_sizes;
    hid_t *column_h5types;
    char **column_names;
    hsize_t num_columns;
    hsize_t num_records;
    char *name;
    {name}_group_{gname}_t parent;
}} {name}_table_{tname};

typedef {name}_table_{tname} * {name}_table_{tname}_t;

"""

code_recordset_header = """typedef struct _{} {{
    {name}_table_{tname}_record_t set;
    size_t num_records;
    void *data_raw;
}} {name}_table_{tname}_recordset;

typedef {name}_table_{tname}_recordset * {name}_table_{tname}_recordset_t;

"""

code_open_table = """{name}_table_{tname}_t {name}_open_table_{tname}({name}_group_{gname}_t group, const char *tname){{
    {name}_table_{tname}_t tb = malloc(sizeof({name}_table_{tname}));
    if( H5TBget_table_info( group->h5_group, tname, &(tb->num_columns), &(tb->num_records) ) < 0 ){{
        printf("table %s not found.\\n",tname);
        free(tb);
        return NULL;
    }}
    tb->name = strdup(tname);
    tb->parent = group;
    tb->column_offsets = calloc(tb->num_columns,sizeof(size_t));
    tb->column_sizes = calloc(tb->num_columns,sizeof(size_t));
    tb->column_names = calloc(tb->num_columns,sizeof(char *));
    for(int i=0;i<tb->num_columns;++i){{
        tb->column_names[i] = calloc(50,sizeof(char));
    }}
    if( H5TBget_field_info( tb->parent->h5_group, tname, tb->column_names, tb->column_sizes, tb->column_offsets, &(tb->record_size) ) < 0 ){{
        printf("error getting field info.\\n");
        for(int i=0;i<tb->num_columns;++i){{
            free(tb->column_names[i]);
        }}
        free(tb->column_names);
        free(tb->column_sizes);
        free(tb->column_offsets);
        free(tb->name);
        free(tb);
        return NULL;
    }}
    return tb;
}}

void {name}_close_table_{tname}({name}_table_{tname}_t tb){{
        for(int i=0;i<tb->num_columns;++i){{
            free(tb->column_names[i]);
        }}
        free(tb->column_names);
        free(tb->column_sizes);
        free(tb->column_offsets);
        free(tb->column_h5types);
        free(tb->name);
        free(tb);
}}

bool {name}_get_records_{tname}({name}_table_{tname}_t table, size_t start, {name}_table_{tname}_recordset_t *records, size_t *num_records){{
    bool need_free = false;
    if( *num_records == 0 ){{
        *num_records = table->num_records-start;
    }}
    if( *records == NULL ){{
        need_free = true;
        *records = malloc(sizeof({name}_table_{tname}_recordset));
    }}
    (*records)->set = calloc(*num_records,sizeof({name}_table_{tname}_record));
    void *data = calloc(*num_records,table->record_size);
    (*records)->data_raw = data;

    if( H5TBread_records( table->parent->h5_group, table->name, start, *num_records, table->record_size, table->column_offsets, table->column_sizes, data ) < 0 ){{
        if( need_free ){{
            free( (*records)->set );
            free( *records );
            *records = NULL;
        }}
        printf("error reading records.\\n");
        return false;
    }}
    for(int i=0;i<*num_records;++i){{
        {name}_table_{tname}_record_t rec = (*records)->set+i;
        void *mydata = data+i*(table->record_size);
        for(int j=0;j<table->num_columns;++j){{
            switch(table->column_names[j][0]){{
{assign_fields}
            }}
        }}
    }}
    return true;
}}

void {name}_close_table_{tname}_recordset({name}_table_{tname}_recordset_t rec){{
    free(rec->set);
    free(rec->data_raw);
    free(rec);
}}

"""

code_include_table = """{name}_table_{tname}_t {name}_open_table_{tname}({name}_group_{gname}_t, const char *);
void {name}_close_table_{tname}({name}_table_{tname}_t);
bool {name}_get_records_{tname}({name}_table_{tname}_t, size_t, {name}_table_{tname}_recordset_t *, size_t *);
void {name}_close_table_{tname}_recordset({name}_table_{tname}_recordset_t);
{name}_table_{tname}_t {name}_create_table_{tname}({name}_group_{gname}_t, const char *);
void {name}_add_records_{tname}({name}_table_{tname}_t, size_t, {name}_table_{tname}_record_t);
"""

code_create_table = """{name}_table_{tname}_t {name}_create_table_{tname}({name}_group_{gname}_t group, const char *tname){{
    herr_t ret;
    {name}_table_{tname}_t table = malloc(sizeof({name}_table_{tname}));
    table->name = strdup(tname);
    table->num_columns = {num_columns};
    table->num_records = 0;
    table->column_offsets = calloc({num_columns},sizeof(size_t));
    table->column_sizes = calloc({num_columns},sizeof(size_t));
    table->column_h5types = calloc({num_columns},sizeof(hid_t));
    table->column_names = calloc({num_columns},sizeof(char *));
    table->parent = group;
{init_columns}
    table->record_size = 0;
    for(int i=0;i<{num_columns};++i){{
        table->column_offsets[i] = table->record_size;
        table->record_size += table->column_sizes[i];
    }}
    ret = H5TBmake_table(tname, group->h5_group, tname, table->num_columns, 0, table->record_size, (const char **)table->column_names, table->column_offsets, table->column_h5types, 20, NULL, 5, NULL);
    if( ret < 0 ){{
        {name}_close_table_{tname}(table);
        return NULL;
    }}
    return table;
}}

"""

code_append_record = """void {name}_add_records_{tname}({name}_table_{tname}_t table, size_t num_records, {name}_table_{tname}_record_t records){{
    void *data_chunk = calloc(num_records,table->record_size);
    for(int i=0;i<num_records;++i){{
        void *data = data_chunk+(i*table->record_size);
{assign_fields}
    }}
    herr_t ret = H5TBappend_records(table->parent->h5_group,table->name,num_records,table->record_size,table->column_offsets,table->column_sizes,data_chunk);
    if( ret < 0 ){{
        printf("failed to append records to table %s\\n",table->name);
    }}
    free(data_chunk);
}}

"""

code_include = """{name}_file_t {name}_open(const char *, const char *, bool);
{name}_file_t {name}_create(const char *);
void {name}_close({name}_file_t);
bool {name}_get_groups({name}_file_t, const char *, char ***, size_t *);
"""

code_include_group = """{name}_group_{gtype}_t {name}_open_group_{gtype}({name}_file_t, const char[]);
{name}_group_{gtype}_t {name}_create_group_{gtype}({name}_file_t, {name}_group_{gtype}_attributes, const char[]);
void {name}_group_{gtype}_attribute_sync({name}_group_{gtype}_t);
void {name}_close_group_{gtype}({name}_group_{gtype}_t group);
"""

code_headers = """#include <hdf5_hl.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "h5_interface_{}.h"

"""

code_open_file = """bool open_file(const char *filename, hid_t *filehandle, bool rw){
    if( access( filename, F_OK ) == 0 ){
        if( rw ){
            *filehandle = H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);
        }else{
            *filehandle = H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT);
        }
        if(*filehandle < 0){
            printf("Error opening file.\\n");
            return false;
        }
    }else{
        printf("File does not exist.\\n");
        return false;
    }
    return true;
}

"""

code_find_groups = """struct _{name}_iterate {{
    char **groups;
    size_t num_groups;
    const char *wildcard;
}};

herr_t {name}_list_groups(hid_t parent, const char *groupname, const H5L_info_t *info, void *data){{
    struct _{name}_iterate *iter = (struct _{name}_iterate *)data;
    if( ( iter->wildcard && strstr(groupname, iter->wildcard) ) || !(iter->wildcard) ){{
        iter->groups[iter->num_groups] = strdup(groupname);
        iter->num_groups++;
    }}
    return 0;
}}

bool {name}_get_groups({name}_file_t lf, const char *group, char ***result, size_t *num_results){{
    if( *result == NULL ){{
        hsize_t num_groups = 0;
        H5Gget_num_objs(lf->root,&num_groups);
        *result = calloc(num_groups,sizeof(char *));
    }}
    struct _{name}_iterate res = {{*result,0,group}};
    hsize_t idx=0;
    H5Literate( lf->root, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, &{name}_list_groups, &res );
    *num_results = res.num_groups;
    return res.num_groups > 0;
}}

"""

code_open_group = """{name}_group_{gtype}_t {name}_open_group_{gtype}({name}_file_t lf, const char gname[]){{
    {name}_group_{gtype}_t group = malloc(sizeof({name}_group_{gtype}));
    group->name = strdup(gname);
    group->parent = lf;
    group->h5_group = H5Gopen(group->parent->root, group->name, H5P_DEFAULT);
    if( group->h5_group < 0 ){{
        printf("group %s not found!\\n",gname);
        free(group->name);
        free(group);
        return NULL;
    }}
{read_attributes}
    return group;
}}

void {name}_group_{gtype}_attribute_sync({name}_group_{gtype}_t group){{
{set_attributes}
}}

{name}_group_{gtype}_t {name}_create_group_{gtype}({name}_file_t lf, {name}_group_{gtype}_attributes attrs, const char gname[]){{
    hid_t gid = H5Gcreate(lf->root, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if( gid < 0 ){{
       printf("failed to create group %s.\\n",gname);
       return NULL;
    }}
    {name}_group_{gtype}_t group = malloc(sizeof({name}_group_{gtype}));
    group->name = strdup(gname);
    group->h5_group = gid;
    group->parent = lf;
    group->attributes = attrs;
    {name}_group_{gtype}_attribute_sync(group);
    return group;
}}

"""

code_close_group = """void {name}_close_group_{gtype}({name}_group_{gtype}_t group){{
    if( group->parent->rw ){{
        {name}_group_{gtype}_attribute_sync(group);
    }}
{free_attributes}
    H5Gclose(group->h5_group);
    free(group->name);
    free(group);
}}

"""

code_silent = "H5Eset_auto(H5E_DEFAULT, NULL, NULL);"

code_open = """{name}_file_t {name}_open(const char *filename, const char *root, bool rw){{
    {silent}
    {name}_file_t lf = malloc(sizeof({name}_file));
    if( !open_file(filename, &(lf->h5_file), rw) ){{
        return NULL;
    }}
    lf->rw = rw;
    if( !root ){{
        root = "/";
    }}
    lf->root = H5Gopen(lf->h5_file,root,H5P_DEFAULT);
    if( lf->root < 0 ){{
        printf("root dir \\"%s\\" not found in file \\"%s\\"\\n",root,filename);
        free(lf);
        return NULL;
    }}
    return lf;
}}

{name}_file_t {name}_create(const char *filename){{
    {name}_file_t lf = malloc(sizeof({name}_file));
    lf->h5_file = H5Fcreate(filename,H5F_ACC_EXCL,H5P_DEFAULT,H5P_DEFAULT);
    if( lf->h5_file < 0 ){{
        printf("file %s exists or cannot be created.\\n",filename);
        free(lf);
        return NULL;
    }}else{{
        lf->root = H5Gopen(lf->h5_file,"/",H5P_DEFAULT);
        lf->rw = true;
        return lf;
    }}
}}

void {name}_close({name}_file_t lf){{
    H5Gclose(lf->root);
    H5Fclose(lf->h5_file);
    free(lf);
}}

"""

code_open_frame = """

"""

#main
if __name__ == "__main__":
    try:
        conf = sys.argv[1]
    except IndexError:
        print("Usage: generate_reader.py <name>.yaml")
        sys.exit(1)
    with open(conf,"r") as conffile:
        config = yaml.load(conffile)
        #create non-mandatory fields
        for group in config["groups"]:
            for att in group["attributes"]:
                try:
                    att["h5name"]
                except KeyError:
                    att["h5name"] = att["name"]
        try:
            config["silent"]
        except KeyError:
            config["silent"] = False
    
    with open("h5_interface_{}.c".format(config["name"]),"w") as sourcefile:
        sourcefile.write(code_headers.format(config["name"]))
        sourcefile.write(code_open_file)
        sourcefile.write(code_open.format(name=config["name"],silent=code_silent if config["silent"] else ""))
        sourcefile.write(code_find_groups.format(name=config["name"]))
        for group in config["groups"]:
            attributes = "    //read attributes\n    herr_t ret;\n"
            attributes_close = "    //free attributes\n"
            attributes_set = "    herr_t ret;\n"
            for att in group["attributes"]:
                try:
                    size = "*".join([int_or_var(name,"group->attributes.") for name in att["shape"]])

                    #for reading
                    attributes += "    group->attributes.{name} = calloc({size},sizeof({dtype}));\n".format(name=att["name"],size=size,dtype=att["type"])
                    attributes += "    ret = H5LTget_attribute(group->h5_group, \".\", \"{h5name}\", {h5type}, group->attributes.{name} );\n".format(name=att["name"],h5type=h5_types[att["type"]],h5name=att["h5name"])
                    attributes_close += "    free(group->attributes.{name});\n".format(name=att["name"])

                    #for writing
                    attributes_set += "    if( group->attributes.{name} == NULL ){{\n".format(name=att["name"]); 
                    try:
                        default = att["default"]
                        make_default = "    if( ret < 0 ){{\n"
                        if( att["type"] == "char" ):
                            attributes_set += "        group->attributes.{name} = strdup(\"{default}\");\n".format(name=att["name"],default=default)
                            make_default += "        free(group->attributes.{name});\n"
                            make_default += "        group->attributes.{{name}} = strdup(\"{default}\");\n".format(default=default)
                        else:
                            attributes_set += "        group->attributes.{name} = calloc({size},sizeof({dtype}));\n".format(name=att["name"],size=size,dtype=att["type"])
                            for i,val in enumerate(att["default"]):
                                attributes_set +="        group->attributes.{name}[{i}] = {val};\n".format(i=i,val=val)
                                make_default += "        group->attributes.{{name}}[{i}] = {val};\n".format(i=i,val=val)
                        make_default += "    }}\n"
                        attributes_set += "    }\n    {\n"
                        attributes += make_default.format(name=att["name"])
                    except KeyError:
                        attributes_set += "    }else{\n";
                        pass
                    if( att["type"] == "char" ):
                        attributes_set += "        ret = H5LTset_attribute_string(group->h5_group, \".\", \"{h5name}\", group->attributes.{name});\n".format(name=att["name"],h5name=att["h5name"],default=default)
                    else:
                        attributes_set += "        ret = H5LTset_attribute_{dtype}(group->h5_group, \".\", \"{h5name}\", group->attributes.{name}, {size});\n".format(dtype=h5_att_types[att["type"]],name=att["name"],h5name=att["h5name"],size=size)
                    attributes_set += "        if( ret < 0 ){{\n            printf(\"failed to set attribute {name}\");\n        }}\n    }}\n".format(name=att["name"])
                except KeyError:
                    attributes += "    ret = H5LTget_attribute(group->h5_group, \".\", \"{h5name}\", {h5type}, &(group->attributes.{name}) );\n".format(name=att["name"],h5name=att["h5name"],h5type=h5_types[att["type"]])
                    attributes_set += "    ret = H5LTset_attribute_{dtype}(group->h5_group, \".\", \"{h5name}\", &(group->attributes.{name}), 1);\n".format(dtype=h5_att_types[att["type"]],name=att["name"],h5name=att["h5name"]);
                    attributes_set += "    if( ret < 0 ){{\n        printf(\"failed to set attribute {name}\");\n    }}\n".format(name=att["name"])
                    try:
                        attributes += "    if( ret < 0 ){{\n        group->attributes.{name} = {default};\n    }}\n".format(name=att["name"],default=str(att["default"]))
                    except KeyError:
                        pass
            sourcefile.write(code_open_group.format(name=config["name"],gtype=group["name"],read_attributes=attributes,set_attributes=attributes_set))
            sourcefile.write(code_close_group.format(name=config["name"],gtype=group["name"],free_attributes=attributes_close))
            for table in group["tables"]:
                assign_fields = ""
                case = None
                init_columns = ""
                assign_columns = ""
                column_index = 0
                for column in sorted(table["columns"],key=lambda e:e["name"]):
                    if case != column["name"][0]:
                        if case is not None:
                            assign_fields += "                  break;\n"
                        case = column["name"][0]
                        assign_fields += "               case '{}':\n".format(case)
                    assign_fields += """                  if( strcmp(table->column_names[j],\"{cname}\") == 0 ){{
                     rec->{cname} = ({ctype} *)(mydata+table->column_offsets[j]);
                  }}\n""".format(cname=column["name"],ctype=column["type"])
                    #code for table creation
                    try:
                        column_size = "*".join([int_or_var(name,"group->attributes.") for name in column["shape"]])
                        column_size_arr = ",".join([int_or_var(name,"group->attributes.") for name in column["shape"]])
                        dims = len(column["shape"])
                        init_columns += "    {{\n        hsize_t array_dims[{dim}] = {{ {sizes} }};\n".format(dim=dims,sizes=column_size_arr)
                        init_columns += "        table->column_h5types[{i}] = H5Tarray_create({h5type},{dim},array_dims);\n    }}\n".format(i=column_index,h5type=h5_types[column["type"]],dim=dims)
                    except KeyError:
                        column_size = "1"
                        init_columns += "    table->column_h5types[{i}] = {h5type};\n".format(i=column_index,h5type=h5_types[column["type"]])
                    init_columns += "    table->column_sizes[{i}] = sizeof({dtype})*{size};\n".format(i=column_index,dtype=column["type"],size=column_size)
                    init_columns += "    table->column_names[{i}] = strdup(\"{cname}\");\n".format(i=column_index,cname=column["name"])
                    #code for record insertion
                    assign_columns += "        memcpy(data+table->column_offsets[{i}],records[i].{cname},table->column_sizes[{i}]);\n".format(i=column_index,cname=column["name"])
                    column_index += 1;
                assign_fields += "                  break;"
                sourcefile.write(code_open_table.format(name=config["name"],gname=group["name"],tname=table["name"],assign_fields=assign_fields));
                sourcefile.write(code_create_table.format(name=config["name"],gname=group["name"],tname=table["name"],num_columns=len(table["columns"]),init_columns=init_columns));
                sourcefile.write(code_append_record.format(name=config["name"],gname=group["name"],tname=table["name"],num_columns=len(table["columns"]),assign_fields=assign_columns));
    
    with open("h5_interface_{}.h".format(config["name"]),"w") as headerfile:
        headerfile.write(code_logfile_header.format(rnd_name(),name=config["name"]))
        for group in config["groups"]:
            headerfile.write("typedef struct _{} {{\n".format(rnd_name()))
            for att in group["attributes"]:
                try:
                    shape = att["shape"]
                    headerfile.write("        {} *{};\n".format(att["type"],att["name"]))
                except KeyError:
                    headerfile.write("        {} {};\n".format(att["type"],att["name"]))
            headerfile.write("}} {name}_group_{gname}_attributes;\n\n".format(name=config["name"],gname=group["name"]))
            headerfile.write("typedef {name}_group_{gname}_attributes * {name}_group_{gname}_attributes_t;\n\n".format(name=config["name"],gname=group["name"]))
            headerfile.write("typedef struct _{} {{\n".format(rnd_name()))
            headerfile.write(code_group_generics.format(name=config["name"],gname=group["name"]))
            headerfile.write("}} {}_group_{};\n\n".format(config["name"],group["name"]))
            headerfile.write("typedef {name}_group_{gname} * {name}_group_{gname}_t;\n\n".format(name=config["name"],gname=group["name"]));
            for table in group["tables"]:
                headerfile.write(code_table_header.format(rnd_name(),name=config["name"],tname=table["name"],gname=group["name"]))
                headerfile.write("typedef struct _{} {{\n".format(rnd_name()))
                for column in table["columns"]:
                    headerfile.write("    {} *{};\n".format(column["type"],column["name"]))
                headerfile.write("}} {}_table_{}_record;\n\n".format(config["name"],table["name"]))
                headerfile.write("typedef {name}_table_{tname}_record * {name}_table_{tname}_record_t;\n\n".format(name=config["name"],tname=table["name"]));
                headerfile.write(code_recordset_header.format(rnd_name(),name=config["name"],tname=table["name"]));
        headerfile.write(code_include.format(name=config["name"]))
        for group in config["groups"]:
            headerfile.write(code_include_group.format(name=config["name"],gtype=group["name"]))
            for table in group["tables"]:
                headerfile.write(code_include_table.format(name=config["name"],gname=group["name"],tname=table["name"]))
