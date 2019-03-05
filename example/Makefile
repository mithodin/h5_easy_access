.PHONY: all clean

debug = -g
libraries = -lhdf5_hl -lhdf5 -lm -ldl -lz
include = -I/opt/HDF_Group/HDF5/1.10.1/include
library_path = -L/opt/HDF_Group/HDF5/1.10.1/lib

all: test

test: test.c h5_interface_bd.c 
	gcc $(debug) -Wall -o test test.c h5_interface_bd.c $(libraries) $(include) $(library_path)

h5_interface_bd.c: bd.yaml
	../generate_interface.py bd.yaml

clean:
	rm -rf h5_interface_bd.c h5_interface_bd.h test dummy.h5
