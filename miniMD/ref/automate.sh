#!/bin/bash
#SBATCH --job-name=optTest
#SBATCH --output=optTest.out
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=16
#SBATCH --time=4:00:00
#SBATCH --mem=4G             # Increased memory to support all runs
#SBATCH --partition=gpu-rgrant
#SBATCH --mail-user=20zdtp@queensu.ca
#SBATCH --mail-type=END,FAIL

./test.sh 8
