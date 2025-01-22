# Distributed System Programming Assignment 2

### How to run the program:
1. Set the working directory to the root of the project.
2. Run the script deploy.sh to create the bucket and  upload the jar files to it.
3. Run the local app using the following command:
```
java -jar local/target/local-1.0.jar [minNpmi] [relativeMinNpmi] [thresholdNpmi] [sampleRate]

minNpmi - the minimum NPMI value to consider a pair of words as a collocation
relativeMinNpmi - the minimum relative NPMI value to consider a pair of words as a collocation
thresholdNpmi - the maximum threshold NPMI value to consider a pair of words as a collocation
sampleRate - the rate of the samples to take from the input files
```
4. Results will be saved in the output folder.

### Implementation Details
This project includes 7 Map-Reduce steps:
1. count-N-cw1w2: Count the number of occurrences of each pair of words and the total number of pairs in the corpus.
2. count-cw1: Count the number of occurrences of each word as the first word in a pair.
3. count-cw2: Count the number of occurrences of each word as the second word in a pair.
4. calculate-NPMI: Calculate the NPMI value for each pair of words.
5. filter-NPMI: Filter the pairs of words that meet the NPMI threshold and minimum values received.
6. sort-NPMI: Sort the pairs of words by their NPMI value (using value2key).
7. top-n: Take the top N pairs of words with the highest NPMI value per decade.

We are using custom keys to improve serialization and deserialization performance. The keys can be found in the common submodule.

### Report
|          Case          | Time  | Map output records | Map output bytes | Combine input records  | Reduce input records | Reduce shuffle bytes |
|:----------------------:|:-----:|:------------------:|:----------------:|:----------------------:|:--------------------:|:--------------------:|
| With local aggregation | 17:29 |    466,669,764     |  11,213,842,706  |      517,415,314       |      51,214,825      |     749,851,359      |
|  Without aggregation   | 19:14 |    466,669,764     |  11,213,842,706  |           0            |     466,669,764      |    2,129,662,436     |
|  More Map tasks(129)   | 22:17 |    466,669,764     |  11,213,842,706  |      466,669,764       |      51,217,834      |     749,851,359      |
|   50% of the corpus    | 14:19 |    233,326,724     |  5,606,731,025   |      233,326,724       |      42,720,515      |     622,840,740      |

### Top 10 Collocations
Top 10 pairs per decade can be found in the Top10.xlsx file.

### Good and bad Collocations
#### Good Collocations
|        Pair        |  NPMI   | Decade |
|:------------------:|:-------:|:------:|
| "בנערינו ובזקנינו" | 0.9990  |  2000  |
|   "בחריש ובקציר"   | 0.9921  |  1850  |
|  "בצלמנו כדומתנו"  | 0.9908  |  1840  |
| "באותות ובמופתים"  | 0.9850  |  1820  |
|     "בני האדם"     | 0.9632  |  1780  |
#### Bad Collocations
|      Pair       |  NPMI  | Decade |
|:---------------:|:------:|:------:|
|  "הנזל וגרטל"   | 0.9958 |  1980  |
| "אלברטוס מגנוס" | 0.9986 |  2000  |
|  "ניםן ותשרי"   | 0.9789 |  1790  |
|  "הנניה מישאל"  | 0.9284 |  1780  |
| "ליצהק וליעקב"  | 0.9773 |  1750  |
