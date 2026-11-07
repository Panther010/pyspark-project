# Data Engineering Assessment

## Overview

This repository contains a simplified version of our data pipeline architecture. We process financial instrument data using configuration-driven transformations.

Your task is to implement a working transformation engine.

## Time

~2 hours

## The Problem

Our analytics team computes derived metrics from raw instrument data. Rather than hardcoding transformation logic, we use YAML configurations that specify what operations to apply.

The configuration in `config/pipeline.yaml` defines a pipeline. Your implementation should execute it.

## Structure

```
├── config/
│   └── pipeline.yaml
├── data/
│   └── input/
├── src/
│   ├── core/
│   └── transforms/
└── tests/
```

## Your Task

Implement the transformation engine. The provided skeleton indicates roughly where code should live, but the design is yours.

## What We're Looking For

- Working code
- Production sensibilities
- Good judgment

## Prerequisites

- Python 3.10+
- Java 11, 17, or 21 (PySpark dependency)

## Running

```bash
pip install -r requirements.txt
pytest tests/
python -m src.main
```

## Questions?

Make reasonable assumptions and document them.
