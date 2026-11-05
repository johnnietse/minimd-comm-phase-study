# Makefile for mpicxx compiler

SHELL = /bin/sh
EXE = miniMD_openmpi 

# Default Options

AVX ?= yes
SIMD ?= yes

#.IGNORE:

# System-specific settings

CC =		mpicxx
CCFLAGS =	-O3 -DMPICH_IGNORE_CXX_SEEK -fopenmp -DNOCHUNK
LINK =		mpicxx 
LINKFLAGS =	-O3 -fopenmp
USRLIB = 	
SYSLIB =	
SIZE =		size

# Check for AVX compile
ifeq ($(AVX), yes)
CCFLAGS += -mavx 
LINKFLAGS += -mavx
endif

ifeq ($(SIMD), yes)
CCFLAGS += -DUSE_SIMD
endif

ifeq ($(ANSI_ALIAS), yes)
CCFLAGS += -ansi-alias 
LINKFLAGS += -ansi-alias
endif

#Check for PAD
ifeq ($(PAD4), yes)
CCFLAGS += -DPAD4 
LINKFLAGS += -DPAD4 
endif

#Check for single precision
ifeq ($(SP), yes)
CCFLAGS += -DPRECISION=1 
LINKFLAGS += -DPRECISION=1 
else
CCFLAGS += -DPRECISION=2
LINKFLAGS += -DPRECISION=2
endif

#Check if debug on
ifeq ($(DEBUG), yes)
CCFLAGS += -g  
LINKFLAGS += -g 
endif

#Use LIBRT for precision timers
ifeq ($(LIBRT),yes)
CCFLAGS += -DPREC_TIMER
USRLIB += -lrt
endif



# Source and Header files
# Add to SRC list
SRC = ljs.cpp input.cpp integrate.cpp atom.cpp force_lj.cpp force_eam.cpp neighbor.cpp \
    thermo.cpp comm.cpp timer.cpp output.cpp setup.cpp comm_monitor.cpp comm_instrumented.cpp

# Add to INC list  
INC = ljs.h atom.h force.h neighbor.h thermo.h timer.h comm.h integrate.h threadData.h variant.h openmp.h \
    force_lj.h force_eam.h types.h comm_monitor.h comm_instrumented.h


OBJ = $(SRC:.cpp=.o)

# Link rule

$(EXE):	$(OBJ)
	$(LINK) $(LINKFLAGS) $(OBJ) $(USRLIB) $(SYSLIB) -o $(EXE)
	$(SIZE) $(EXE)

# Compilation rules

.cpp.o:
	$(CC) $(CCFLAGS) -E $*.cpp > $*.2.cpp
	$(CC) $(CCFLAGS) -c $*.2.cpp -o $*.o

# Individual dependencies

$(OBJ): $(INC)


clean:
	rm -f *.o *.2.cpp $(EXE)
