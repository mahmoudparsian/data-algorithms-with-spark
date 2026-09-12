# Chapter 10: Data Design Patterns

### Mahmoud Parsian

This directory holds the "Data Design Patterns" chapter/paper along
with its companion PySpark and Scala/Spark example programs.

## Documents

| File | Description |
|------|-------------|
| [`data_design_patterns.md`](data_design_patterns.md) | The chapter text in Markdown (source of truth — edit this one). |
| [`data_design_patterns.html`](https://htmlpreview.github.io/?https://github.com/mahmoudparsian/data-algorithms-with-spark/blob/master/code/chap10/data_design_patterns.html) | HTML rendering of the chapter ([raw file](data_design_patterns.html)), generated from the Markdown source. GitHub doesn't render `.html` files directly, so this link goes through [htmlpreview.github.io](https://htmlpreview.github.io) to display it properly. |
| [`data_design_patterns.pdf`](data_design_patterns.pdf) | PDF rendering of the chapter, generated from the Markdown source. |

## What's covered

The chapter presents Data Design Patterns in an informal,
pragmatic way, organized into:

1. **Summarization Patterns** — `groupByKey()`, `reduceByKey()`,
   `combineByKey()`, and DataFrame aggregations for computing
   `(min, max, count, ...)`-style statistics, with and without keys.
2. **In-Mapper-Combiner Pattern** — reducing the number of
   `(key, value)` pairs emitted by mappers, illustrated with a
   DNA base-count (FASTA) example across three implementations
   (classic MapReduce, in-mapper-combiner, `mapPartitions()`).
3. **Filtering Patterns** — filtering RDDs and DataFrames
   (`filter()`, `where()`).
4. **Organization Patterns** — the Structured-to-Hierarchical
   pattern (joining and reshaping flat records into XML/JSON) and
   the Partitioning/Binning pattern (`Bucketizer`, categorical bins).
5. **Join Patterns** — overview of Spark's join strategies (shuffle
   hash, sort-merge, broadcast); full coverage is in Chapter 11 of
   *Data Algorithms with Spark*.
6. **Meta Patterns** — patterns about patterns (job chaining/merging,
   ML pipelines).
7. **Input/Output Patterns** — input format and schema
   considerations, and output patterns (`partitionBy()`, file-count
   control, format choice).

## Source code

* [`python/`](python) — PySpark implementations of the patterns
  above (summarization via `groupByKey`/`reduceByKey`/`combineByKey`/
  `aggregateByKey`, in-mapper-combiner and `mapPartitions()` DNA
  base-count solutions, min/max via `mapPartitions()`,
  structured-to-hierarchical XML generation, and top-N examples).
  Each `.py` script has a matching `.sh` launcher.
* [`scala/`](scala) — Scala/Spark port of the same examples, built
  with Gradle (`build.gradle`, `gradlew`); see
  [`scala/README.md`](scala/README.md) and
  `scala/run_spark_applications_scripts/` for run scripts.
* [`tools/`](tools) — `build_docs.sh` and its helper files, used to
  regenerate `data_design_patterns.html`/`.pdf` from the Markdown
  source (see [Regenerating the HTML and PDF](#regenerating-the-html-and-pdf) below).

## References

* [Data Algorithms with Spark](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/) (O'Reilly)
* [PySpark Algorithms](https://github.com/mahmoudparsian/pyspark-algorithms)
* [PySpark Tutorial](https://github.com/mahmoudparsian/pyspark-tutorial)

## Regenerating the HTML and PDF

After editing `data_design_patterns.md`, regenerate both outputs with:

```
tools/build_docs.sh          # regenerate both .html and .pdf
tools/build_docs.sh html     # regenerate only the .html
tools/build_docs.sh pdf      # regenerate only the .pdf
```

This requires [pandoc](https://pandoc.org) on `PATH`, plus `xelatex`
(from a TeX Live / MacTeX install) for the PDF target. The script uses
two helper files in [`tools/`](tools): `github_ids.lua` (a Pandoc Lua
filter so the chapter's hand-written table-of-contents anchors resolve
correctly) and `pandoc_header.html` (a self-contained, GitHub-styled
CSS include for the HTML build).

If you'd rather run/tweak the commands by hand instead of using the
script, here's what it does under the hood:

```
# HTML
pandoc data_design_patterns.md \
  --to=html5 \
  --standalone \
  --highlight-style=pygments \
  --lua-filter=tools/github_ids.lua \
  --metadata pagetitle="Data Design Patterns" \
  -H tools/pandoc_header.html \
  -o data_design_patterns.html

# PDF (the raw-HTML title/byline block at the top of the .md is stripped
# and re-supplied as YAML title/author/date metadata first, since
# Pandoc's LaTeX writer can't render raw HTML — see tools/build_docs.sh
# for the exact preprocessing step)
pandoc data_design_patterns.md \
  --to=pdf \
  --pdf-engine=xelatex \
  --highlight-style=pygments \
  --lua-filter=tools/github_ids.lua \
  -V geometry:margin=1in \
  -V colorlinks=true \
  -V linkcolor=blue \
  -V urlcolor=blue \
  --toc --toc-depth=2 \
  -o data_design_patterns.pdf
```
