# h5_easy_access
Generate a simple C interface for hdf5 files

This is currently in early development. Reading is sort of complete, writing only supports group creation with attributes at this time.

Check out the example to get an idea how this works. Documentation is going to come.
In short:
 - write a .yaml file to pass to the python script
 - include the generated header file into your c program
 - read the header file to see what you can do
