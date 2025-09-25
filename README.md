# Co-Purchase Analysis

A distributed co-purchase analysis system using Scala and Apache Spark to analyze product affinities in e-commerce datasets.
For complete project details, methodology, and performance analysis, see the [full project report](https://monk-cup.s3.eu-north-1.amazonaws.com/CO_PURCHASE_ANALYSIS+(2).pdf).

## Project Overview

This project implements a MapReduce-based algorithm to calculate co-occurrence frequencies of product pairs within purchase orders. The analysis identifies products that are frequently bought together, providing valuable insights for recommendation systems.

## Technology Stack

- **Scala**: 2.12.18
- **Apache Spark**: 3.5.0
- **Build Tool**: sbt
- **Cloud Platform**: Google Cloud DataProc

## Input Format

CSV file with `order_id,product_id` format:
```
1,12
1,14
2,8
2,12
```

## Running Locally

1. Start sbt console:
   ```bash
   sbt
   ```

2. Run with input/output paths:
   ```bash
   run data/orders.csv data/output_local
   ```

## Running on Google Cloud DataProc

### Prerequisites
- Google Cloud SDK installed and authenticated
- DataProc API enabled
- Cloud Storage bucket created via web interface
- JAR and CSV files uploaded to bucket via web interface

### Create Cluster

**Single-node:**
```bash
gcloud dataproc clusters create cpa-cluster-1 \
  --region us-central1 \
  --master-boot-disk-size 240 \
  --project your-project-id \
  --single-node
```

**Multi-worker:**
```bash
gcloud dataproc clusters create cpa-cluster-2 \
  --region=us-central1 \
  --master-boot-disk-size=240 \
  --num-workers=2 \
  --worker-boot-disk-size=240 \
  --project=your-project-id
```

### Submit Job

```bash
gcloud dataproc jobs submit spark \
  --cluster=cpa-cluster-1 \
  --region=us-central1 \
  --jar=gs://your-bucket-name/co-purchase-analysis_2.12-1.0.0.jar \
  -- gs://your-bucket-name/orders.csv \
     gs://your-bucket-name/output_1
```

### Clean Up

```bash
gcloud dataproc clusters delete cpa-cluster-1 --region us-central1
```

## Output Format

CSV with co-purchase frequencies:
```
product_id_1,product_id_2,count
13176,47209,62341
13176,21137,61628
```
