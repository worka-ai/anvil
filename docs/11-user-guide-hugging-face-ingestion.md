---
slug: /anvil/user-guide/hugging-face-ingestion
title: 'User Guide: Hugging Face Ingestion'
description: Learn how to ingest models from the Hugging Face Hub directly into your Anvil cluster.
tags: [user-guide, hugging-face, models, ingestion, cli]
---

# Chapter 11: Hugging Face Ingestion

> **TL;DR:** Use the `anvil hf ingest` command to quickly and efficiently pull model repositories from the Hugging Face Hub and store them as objects in your Anvil cluster.

Anvil provides a streamlined workflow for ingesting machine learning models directly from the Hugging Face Hub. This feature is designed to simplify the process of populating your object store with the models you need for your AI applications.

### How It Works

When you initiate an ingestion, Anvil performs the following steps:

1.  **API Interaction:** It communicates with the Hugging Face Hub API to get the list of all files associated with the specified model repository.
2.  **Concurrent Download:** It downloads the model files concurrently to maximize speed and efficiency.
3.  **Object Storage:** As each file is downloaded, it is streamed directly into your Anvil cluster as an object. The object key is automatically determined based on the file's path in the original repository.

This process is significantly faster and more reliable than manually downloading files and then uploading them.

### Using the `anvil`

The primary way to use this feature is through the `anvil`.

**Command**

```bash
anvil hf ingest --repo <REPO_ID> --bucket <BUCKET_NAME>
```

-   `--repo`: The ID of the repository on the Hugging Face Hub (e.g., `gpt2` or `stabilityai/stable-diffusion-2-1`).
-   `--bucket`: The name of the Anvil bucket where the model files will be stored.

**Example**

Let's say you want to ingest the original GPT-2 model into a bucket named `llm-models`.

```bash
# First, ensure the destination bucket exists
anvil bucket create --name llm-models --region europe-west-1

# Now, ingest the model
anvil hf ingest --repo gpt2 --bucket llm-models
```

After the process completes, you can list the objects in your bucket to see the model files:

```bash
anvil object ls --path s3://llm-models/gpt2/
```

You will see all the files from the `gpt2` repository, such as `config.json`, `model.safetensors`, and `tokenizer.json`, stored as objects in your Anvil cluster.
