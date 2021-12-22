# Chapter 2

## DNA-Base-Count Programs using FASTA Input Format

Using FASTA input files, there are 3 versions of DNA-Base-Count

* Version-1:
    * Uses basic MapReduce programs
    * Using PySpark (`DNA-FASTA-V1/dna_base_count_ver_1.py`)

* Version-2:
    * Uses InMapper Combiner design pattern
    * Using PySpark (`DNA-FASTA-V2/dna_base_count_ver_2.py`)

* Version-3:
    * Uses InMapper Combiner design pattern (by using mapPartitions() transformations)
    * Using PySpark (`DNA-FASTA-V3/dna_base_count_ver_3.py`)


## DNA-Base-Count Programs using FASTQ Input Format

Using FASTQ input files, the following solution is available:

* Uses InMapper Combiner design pattern (by using mapPartitions() transformations)
* Using PySpark (`DNA-FASTQ/dna_base_count_fastq.py`)


## FASTA Files to Test DNA-Base-Count

* A small sample FASTA file (`data/sample.fasta`) is provided.

* To test DNA-Base-Count programs with large size FASTA files,
you may download them from here:


````
	ftp://ftp.ensembl.org/pub/release-91/fasta/homo_sapiens/dna/

	ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/rs_fasta/
	
	ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	
````
