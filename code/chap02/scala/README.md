# Chapter 2

## DNA-Base-Count Programs using FASTA Input Format

Using FASTA input files, there are 3 versions of DNA-Base-Count

* Version-1:
    * Uses basic MapReduce programs
    * Using Spark (`org.data.algorithms.spark.ch02.DNABaseCountVER1`)

* Version-2:
    * Uses InMapper Combiner design pattern
    * Using PySpark (`org.data.algorithms.spark.ch02.DNABaseCountVER2`)

* Version-3:
    * Uses InMapper Combiner design pattern (by using mapPartitions() transformations)
    * Using PySpark (`org.data.algorithms.spark.ch02.DNABaseCountVER3`)


## DNA-Base-Count Programs using FASTQ Input Format

Using FASTQ input files, the following solution is available:

* Uses InMapper Combiner design pattern (by using mapPartitions() transformations)
* Using PySpark (`org.data.algorithms.spark.ch02.DNABaseCountFastq`)


## FASTA Files to Test DNA-Base-Count

* A small sample FASTA file (`data/sample.fasta`) is provided.

* To test DNA-Base-Count programs with large size FASTA files,
you may download them from here:


````
	ftp://ftp.ensembl.org/pub/release-91/fasta/homo_sapiens/dna/

	ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/rs_fasta/

	ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
````
