#-----------------------------------------------------------------------
# This file compiles for OpenMP and MPI hybrid operations using the GNU
# compile chain.



# Makefile for memory-bound and serial phase testing

.PHONY: all setup memory serial clean help

all: setup memory serial

setup:
	@echo "Setting up test environment..."
	chmod +x *.sh *.py
	@./setup_test_environment.sh

memory:
	@echo "Starting memory-bound phase testing..."
	@./memory_bound_phase_test.sh

serial:
	@echo "Starting serial computation phase testing..."
	@./serial_computation_phase_test.sh

clean:
	@echo "Cleaning test results..."
	rm -f *.log *.json *.png
	rm -rf results/

help:
	@echo "Available targets:"
	@echo "  setup   - Setup test environment and dependencies"
	@echo "  memory  - Run memory-bound phase testing"
	@echo "  serial  - Run serial computation phase testing"
	@echo "  all     - Run setup, then both test suites"
	@echo "  clean   - Remove all generated files"
	@echo "  help    - Show this help message"

MINIFE_TYPES =  \
        -DMINIFE_SCALAR=double   \
        -DMINIFE_LOCAL_ORDINAL=int      \
        -DMINIFE_GLOBAL_ORDINAL=int

MINIFE_MATRIX_TYPE = -DMINIFE_CSR_MATRIX
#MINIFE_MATRIX_TYPE = -DMINIFE_ELL_MATRIX

#-----------------------------------------------------------------------

CFLAGS = -O3 -fopenmp
CXXFLAGS = $(CFLAGS)

CPPFLAGS = -I. -I../utils -I../fem $(MINIFE_TYPES) \
	$(MINIFE_MATRIX_TYPE) \
	-DHAVE_MPI -DMPICH_IGNORE_CXX_SEEK \
	-DMINIFE_REPORT_RUSAGE

LDFLAGS=$(CFLAGS)
LIBS=

# The MPICH_IGNORE_CXX_SEEK macro is required for some mpich versions,
# such as the one on my cygwin machine.

#CXX=mpiicpc
#CC=mpiicc

#CXX=g++
#CC=g++

#CXX=icpc
#CC=icc

CXX=mpicxx
CC=mpicc

include make_targets
