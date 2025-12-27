# Distributed Systems Programming - Assignment 2
## Collocation Extraction (Top 100 per Decade) using Hadoop MapReduce on AWS EMR

---

## STUDENT INFORMATION

| Name |
|------|
| naser assi |
| Wesam gara |

---

## 1. PROJECT OVERVIEW

### Goal
Extract the top 100 collocations for each decade, for BOTH English and Hebrew using Google N-Grams (2-grams), ranked by Log-Likelihood Ratio (LLR).

### Definition
A **collocation** is a pair of ordered words (w1, w2) that co-occur more often than expected by chance.

### Key Requirements
1. The system must be **scalable** and must NOT assume that any decade's word pairs or unigram lists can fit in memory.
2. Avoid generating **redundant key-value pairs**.
3. Filter **stop words** from both unigrams and bigrams.

---

## 2. DATA SOURCES (AWS S3)

### Bigrams (2-gram datasets)
| Language | Path |
|----------|------|
| English | `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data` |
| Hebrew | `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data` |

### Unigrams (1-gram datasets)
| Language | Path |
|----------|------|
| English | `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data` |
| Hebrew | `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data` |

### Format
- **File Type**: SequenceFile with LZO block compression
- **InputFormat**: `SequenceFileInputFormat`

---

## 3. STOP WORDS FILTERING

We filter stop words at the **Mapper level** for BOTH unigrams and bigrams.

### Filtering Rules
| Input Type | Rule |
|------------|------|
| Unigram | If word is a stop word → skip entirely |
| Bigram | If either w1 OR w2 is a stop word → discard entire bigram |

---

## 4. LOG-LIKELIHOOD RATIO (LLR) METRIC

### Variables
```
c1  = count(w1)         - occurrences of first word
c2  = count(w2)         - occurrences of second word  
c12 = count(w1 w2)      - occurrences of the bigram
N   = total word count  - total words in corpus for that decade
```

### LLR Calculation (Binomial Likelihood Ratio)
```
p  = c2 / N
p1 = c12 / c1
p2 = (c2 - c12) / (N - c1)

LLR = -2 × (logL(c12, c1, p) + logL(c2-c12, N-c1, p) 
         - logL(c12, c1, p1) - logL(c2-c12, N-c1, p2))

where logL(k, n, x) = k×log(x) + (n-k)×log(1-x)
```

---

## 5. MAPREDUCE DESIGN (4 JOBS)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ JOB 1: Calculate N (Total Word Count per Decade)                            │
│ Input: 1-gram file                                                          │
│ Output: decade → total_count                                                │
│ Optimization: Combiner for local aggregation                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ JOB 2: Join c1 (unigram count) with c12 (bigram count)                      │
│ Input: 1-gram + 2-gram files (MultipleInputs)                               │
│ Output: decade → w1 w2 c12 c1                                               │
│ Technique: Secondary Sort (tag 0 for c1, tag 1 for bigrams)                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ JOB 3: Join c2 and Calculate LLR                                            │
│ Input: Job 2 output + 1-gram file                                           │
│ Output: "decade w1 w2" → LLR_score                                          │
│ Technique: Secondary Sort (join on w2), N passed via Configuration          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ JOB 4: Sort and Select Top 100                                              │
│ Input: Job 3 output                                                         │
│ Output: Top 100 collocations per decade, sorted by LLR descending           │
│ Technique: Secondary Sort (by decade, then score desc), PriorityQueue       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Techniques
- **Secondary Sort**: Ensures related records arrive together and in correct order
- **MultipleInputs**: Read from both 1-gram and 2-gram files in same job
- **Combiner**: Local aggregation in Job 1 to reduce shuffle size
- **Streaming Aggregation**: Process records without loading all into memory

---

## 6. STATISTICS

Statistics from Hadoop job counters, comparing WITH and WITHOUT local aggregation (Combiner).

### Job 1: Calculate N (English Dataset)

| Metric | WITH Combiner | WITHOUT Combiner |
|--------|---------------|------------------|
| Map Output Records | 291,089,410 | 291,089,410 |
| Combine Input Records | 291,089,410 | 0 |
| Combine Output Records | 4,042 | 0 |
| Reduce Input Records | 4,042 | 291,089,410 |
| Reduce Output Records | 43 | 43 |

### Network Traffic Reduction
```
Records sent to Reducer WITHOUT Combiner: 291,089,410
Records sent to Reducer WITH Combiner:    4,042

Reduction = (291,089,410 - 4,042) / 291,089,410 × 100%
          = 99.9986%
```

### All Jobs Summary (English)

| Job | Description | Map Output | Reduce Input | Reduce Output |
|-----|-------------|------------|--------------|---------------|
| Step 1 | Calculate N | 291,089,410 | 4,042 | 43 |
| Step 2 | Join c1+c12 | 2,075,169,927 | 2,075,169,927 | 349,065,024 |
| Step 3 | Calculate LLR | 640,154,434 | 640,154,434 | 349,065,024 |
| Step 4 | Top 100 | 81,717 | 81,717 | 4,285 |

### Conclusion
Local aggregation (Combiner) reduced network traffic by **99.9986%** in Step 1, from ~291 million records to just ~4,000 records.

---

## 7. MANUAL OUTPUT ANALYSIS

### A) 10 GOOD Collocations (Correctly Identified)

#### English (5)

| # | Decade | Collocation | LLR | Why It's Good |
|---|--------|-------------|-----|---------------|
| 1 | 1980 | united states | 56,814,393 | Proper noun - country name, fixed entity |
| 2 | 1980 | new york | 87,834,450 | Proper noun - city name, geographic entity |
| 3 | 1920 | supreme court | 2,770,383 | Institutional name - legal/government entity |
| 4 | 1920 | civil war | 1,862,029 | Historical event - fixed compound noun |
| 5 | 1950 | world war | 3,806,852 | Historical event - globally recognized term |

#### Hebrew (5)

| # | Decade | Collocation | LLR | Why It's Good |
|---|--------|-------------|-----|---------------|
| 1 | 1980 | ראש הממשלה | 124,521 | Prime Minister - political title, named entity |
| 2 | 1950 | ההסתדרות הציונית | 99,238 | Zionist Organization - institutional name |
| 3 | 1860 | בית המקדש | 5,853 | Temple - religious/historical landmark |
| 4 | 1980 | תל אביב | 80,467 | Tel Aviv - city name, geographic entity |
| 5 | 1980 | האוניברסיטה העברית | 121,663 | Hebrew University - institutional name |

---

### B) 10 BAD Collocations (Incorrectly Identified)

#### English (5)

| # | Decade | Collocation | LLR | Why It's Bad |
|---|--------|-------------|-----|--------------|
| 1 | 1650 | fo far | 487 | **OCR Error**: Should be "so far". The long 's' (ſ) was misread as 'f' |
| 2 | 1650 | jefus chrift | 138 | **OCR Error**: Should be "Jesus Christ". Letters misread from old printing |
| 3 | 1800 | thou hast | 113,739 | **Archaic Grammar**: Second-person conjugation, not a semantic unit |
| 4 | 1980 | et al | 45,339,171 | **Citation Abbreviation**: Academic reference marker, not meaningful phrase |
| 5 | 1980 | ve got | 3,558,421 | **Contraction Fragment**: Part of "I've got", incomplete phrase |

#### Hebrew (5)

| # | Decade | Collocation | LLR | Why It's Bad |
|---|--------|-------------|-----|--------------|
| 1 | 1890 | נוםעי הצלב | 17,820 | **OCR Error**: Should be "נוסעי הצלב" (The Crusaders). The letter 'ס' was misread as 'ם'. |
| 2 | 1620 | ואהר כך | 249 | **OCR Error**: Should be "ואחר כך" (And afterwards). The letter 'ח' was misread as 'ה'. |
| 3 | 1620 | כרי שלא | 159 | **OCR Error**: Should be "כדי שלא" (In order not to). The letter 'ד' was misread as 'ר'. |
| 4 | 1620 | שלא יהא | 233 | **Grammatical Fragment**: "That there shall not be" - Halachic function phrase, not a distinct entity. |
| 5 | 1680 | ראה נא | 90 | **Discourse Marker**: "See please" - Directs reader's attention, not a meaningful concept. |

---

### C) Root Cause Analysis for Bad Collocations

Bad collocations are **not errors in the LLR computation**, but rather a known limitation of purely statistical association measures. Common causes:

| Cause | Description | Examples |
|-------|-------------|----------|
| **OCR Errors** | Scanned books contain recognition mistakes from old fonts | "fo far", "jefus chrift", "muft needs" |
| **Archaic Grammar** | Old English constructions that passed modern stop-word filters | "thou hast", "thou art", "wilt thou" |
| **Citation Artifacts** | Academic reference patterns | "et al", "et seq" |
| **Grammatical Connectors** | High-frequency function phrases | "ואחר כך", "כדי שלא", "אלא גם" |
| **Contraction Fragments** | Incomplete words from contractions | "ve got" (from "I've got") |

**Key Insight**: The Log-Likelihood Ratio captures strong co-occurrence patterns regardless of semantic meaning. Manual linguistic analysis is required to distinguish statistically strong but semantically weak collocations from meaningful lexical units.

---

## 8. HOW TO RUN

### Prerequisites
- Java 8+
- Maven
- AWS CLI configured
- S3 bucket for JAR, logs, and output

### Build
```bash
mvn clean package
```

### Upload JAR to S3
Upload `target/assignment2-1.0-SNAPSHOT.jar` to `s3://naser-collocation-bucket/` via AWS Console.

### EMR Step Configuration

| Field | Value |
|-------|-------|
| JAR Location | `s3://naser-collocation-bucket/assignment2-1.0-SNAPSHOT.jar` |
| Main Class | `com.collocation.Main` |

#### English Arguments
```
s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data s3://naser-collocation-bucket/output_english eng
```

#### Hebrew Arguments
```
s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data s3://naser-collocation-bucket/output_hebrew heb
```

### EMR Cluster Configuration

| Setting | Value |
|---------|-------|
| Instance Type | m5.xlarge |
| Instance Count | 8 |
| Release | emr-6.15.0 |
| Application | Hadoop |

### Estimated Runtime & Cost
| Corpus | Time | Cost |
|--------|------|------|
| English | ~58 minutes | ~$2.00 |
| Hebrew | ~7 minutes | ~$0.25 |

---

## 9. OUTPUT LOCATIONS

### S3 Paths

| Corpus | Output Location |
|--------|----------------|
| English | `s3://naser-collocation-bucket/output_english/final_output/` |
| Hebrew | `s3://naser-collocation-bucket/output_hebrew/final_output/` |
| Logs | `s3://naser-collocation-bucket/j-AXBPLDNTKY4F/` |

### Output Format
```
<decade> <word1> <word2>    <LLR_score>
```

Example:
```
1980 new york    87834450.24
1980 united states    56814393.12
1950 world war    3806852.37
```

---

## 10. COST & DEVELOPMENT NOTES

- During debugging, tested on small subsets before running full corpus
- Used minimal cluster sizes during development to reduce cost
- Final runs used m5.xlarge × 8 instances for optimal performance
- All EMR clusters terminated immediately after completion

---

## 11. KNOWN LIMITATIONS

1. **Statistical vs Semantic**: LLR captures statistical association, not semantic meaning
2. **OCR Quality**: Google Books corpus contains OCR errors from scanned documents
3. **Stop Word Coverage**: Our stop word list may not cover all domain-specific function words
4. **Archaic Language**: Modern stop word lists don't filter archaic English (thou, thee, hast)
