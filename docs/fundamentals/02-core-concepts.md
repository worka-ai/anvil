---
slug: /fundamentals/core-concepts
title: 'Core Concepts'
description: An explanation of the fundamental concepts in Anvil - Tenants, Buckets, Objects, and Apps.
tags: [fundamentals, concepts, tenants, buckets, objects]
---

# Chapter 2: Core Concepts

> **TL;DR:** Anvil organizes data into `Buckets`, which belong to `Tenants`. Objects within buckets are identified by a `key`. Access is controlled by `Apps` with specific API keys.

Before you can use Anvil effectively, it's important to understand its fundamental concepts. These primitives provide the structure for organizing your data and securing access to it.

### 2.1. Tenants: Your Isolated Workspace

A **Tenant** is the top-level container in Anvil, representing a single user, team, or customer. All other resources, including buckets and apps, belong to a tenant.

-   **Isolation:** Tenants are completely isolated from one another. A user or application operating within one tenant cannot see or access the buckets or objects of another tenant.
-   **Ownership:** The tenant is the ultimate owner of all its resources.

When you first set up Anvil, you create an initial tenant. In a larger deployment, you would create a new tenant for each distinct customer or internal team that will use the storage system.

### 2.2. Buckets: Organizing Your Objects

A **Bucket** is a container for your objects, similar to a folder or directory in a traditional filesystem. 

-   **Unique Naming:** Bucket names must be unique across the entire Anvil deployment, not just within your tenant.
-   **Regionality:** Each bucket is associated with a specific **Region**. All objects within that bucket will be stored on peers operating in that region.
-   **Public Access:** By default, all buckets are private. However, you can configure a bucket to allow public, anonymous read access to its objects.

### 2.3. Objects: The Data You Store

An **Object** is the fundamental unit of data in Anvil. It consists of two main parts:

-   **Key:** The unique identifier for the object within a bucket (e.g., `path/to/my-file.jpg`).
-   **Data:** The actual content of the file you are storing.

Anvil also stores metadata for each object, such as its size, content type, and an ETag (a hash of its content).

### 2.4. Apps & API Keys: Programmatic Access

To interact with Anvil programmatically, you don't use your main tenant credentials directly. Instead, you create an **App** within your tenant.

-   **Credentials:** When you create an app, Anvil generates a `Client ID` and a `Client Secret`. These are the credentials you will use to authenticate with the S3 gateway or the gRPC API.
-   **Permissions:** Apps are granted permissions via **Policies**. A policy links an app to a specific set of actions (e.g., `read`, `write`) on a resource (e.g., a specific bucket or a pattern of objects). This allows you to follow the principle of least privilege, giving your applications only the access they need.
