![K-mer](https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/images/kmer.jpg)

----

![K-mer](https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/images/kmer_4.png)

-----

K-mer
=====
In bioinformatics, k-mers are substrings of length 
`k` contained within a biological sequence. Primarily 
used within the context of computational genomics and 
sequence analysis, in which k-mers are composed of 
nucleotides (i.e. `A`, `T`, `G`, and `C`), k-mers are 
capitalized upon to assemble DNA sequences, improve 
heterologous gene expression, identify species in 
metagenomic samples, and create attenuated vaccines. 
Usually, the term k-mer refers to all of a sequence's 
subsequences of length `k`, such that the sequence 
`AGAT` would have four monomers (`A`, `G`, `A`, and `T`), 
three 2-mers (`AG`, `GA`, `AT`), two 3-mers (`AGA` and 
`GAT`) and one 4-mer (`AGAT`). More generally, a sequence 
of length `L` will have `L-k+1` k-mers and `n^k` total 
possible k-mers, where `n` is number of possible monomers 
(e.g. four in the case of DNA).

The term k-mer typically refers to all the possible 
substrings of length `k` that are contained in a string. 
In computational genomics, k-mers refer to all the possible 
subsequences (of length `k`) from a read obtained through 
DNA Sequencing. The amount of k-mers possible given a string 
of length, `L`, is `L-k+1` whilst the number of possible 
k-mers given n possibilities (4 in the case of DNA e.g. ACTG) 
is `n^k`. K-mers are typically used during sequence assembly, 
but can also be used in sequence alignment. In the context of 
the human genome, k-mers of various lengths have been used to 
explain variability in mutation rates. 

-----

References
==========

* [k-mer Wikipedia](https://en.wikipedia.org/wiki/K-mer)

* [k-mer counting, part I: Introduction, by Bernardo J. Clavijo](https://bioinfologics.github.io/post/2018/09/17/k-mer-counting-part-i-introduction/)

