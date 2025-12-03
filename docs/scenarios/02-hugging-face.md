---
slug: /scenarios/hugging-face
title: 'Scenario: Hugging Face Model Ingestion'
description: A complete guide to ingesting models from the Hugging Face Hub into Anvil.
tags: [scenario, cli, hugging-face, models]
---

# Scenario: Hugging Face Model Ingestion

This guide covers the end-to-end workflow for using Anvil's built-in Hugging Face integration to ingest and store ML models.

**Key Concept:** Ingestion is an **asynchronous** background job. You initiate the job with the CLI, and Anvil's backend workers handle the download and storage process. You can then use the CLI to monitor the job's status.

### 1. Admin: Grant Hugging Face Permissions

First, an administrator must grant the client's app the necessary permissions to manage Hugging Face keys and initiate ingestion jobs.

```bash
# Grant permission to manage HF keys and start ingestions
admin policy grant --app-name data-science-app --action hf:key-create --resource "*"
admin policy grant --app-name data-science-app --action hf:key-delete --resource "*"
admin policy grant --app-name data-science-app --action hf:key-read --resource "*"
admin policy grant --app-name data-science-app --action hf:ingest-start --resource "*"
```

### 2. Client: Manage Hugging Face API Keys

The client needs to add their Hugging Face API token to Anvil. This token is stored securely and associated with a friendly name.

```bash
# Add a personal HF token to Anvil and give it a memorable name
anvil hf key add --name my-hf-key --token hf_abc...xyz --note "Personal Read Token"

# List available HF keys to confirm it was added
anvil hf key ls
```
To remove the key later:
```bash
anvil hf key rm --name my-hf-key
```

### 3. Client: Prepare Destination and Start Ingestion

Before starting the ingestion, the client must ensure a destination bucket exists and that the administrator has granted `object:write` permissions for it.

```bash
# admin policy grant --app-name data-science-app --action object:write --resource "models/*"
```

The `ingest start` command will return a unique ID for the ingestion job.

**Expected Output:**
```
ingestion id: ingest_12345...
```

### 4. Client: Monitor the Ingestion Job

The client uses the job ID to check the status of the asynchronous ingestion process.

```bash
# Check the status of the background job
anvil hf ingest status --id ingest_12345...
```
**Expected Output:**
```
state=Completed queued=0 downloading=0 stored=14 failed=0 error=
```

If necessary, the job can be cancelled before it completes:
```bash
anvil hf ingest cancel --id ingest_12345...
```

Once complete, the model files will be available as objects in the `models` bucket.