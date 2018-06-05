# Examples

This directory contains example Kappa programs.

Before you try out these programs, make sure that the `coordinator` binary is in your `PATH`.
Try invoking the coordinator from the command line:

```console
user:./$ coordinator --help
Usage of coordinator:
  -config string
    	configuration file for the platform; auto-detected if unspecified
  -env value
    	environment variables to pass to handler, e.g., "--env KEY1=value1 --env KEY2=value2"
  -event string
      	application event (in JSON) (default "{}")
  ...
```

Each example program comes with a "run" script (usually named either `run.py` or
`run.sh`).  For usage instructions, invoke the run script with no arguments and/or
read the documentation in the script.

For a description of an example program, see the docstring of the program's Python source.
