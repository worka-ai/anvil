---
slug: /scenarios/admin-tasks
title: 'Scenario: Administrative Tasks'
description: A guide covering miscellaneous administrative commands like creating admin users and resetting app secrets.
tags: [scenario, cli, admin, users]
---

# Scenario: Common Administrative Tasks

This guide covers several standalone administrative commands for managing users and application credentials.

### 1. Admin: Create a New Administrator User

To give another person administrative access to Anvil, you can create a new admin user for them. This is distinct from tenants and apps; it is for managing the Anvil system itself.

```bash
# Create a new administrator with a password
anvil-admin user create \
  --username new-admin \
  --email admin@acme.com \
  --password "a-very-strong-password" \
  --role administrator
```

### 2. Admin: Reset an App's Client Secret

If an application's client secret is compromised, you can immediately invalidate it and generate a new one. This is a critical security feature.

```bash
# Immediately invalidate an app's secret and generate a new one
anvil-admin app reset-secret --app-name data-science-app
```
The command will output a new `Client Secret`. The old secret will no longer work.

### 3. Client: Interactive CLI Configuration

While non-interactive configuration is recommended for scripts, users can use the `configure` command for a wizard-style setup experience.

```bash
anvil configure
```
This will prompt the user for the profile name, host, Client ID, and Client Secret.

**Example Interaction:**
```
? Profile Name: acme
? Anvil Host: https://anvil.acme.com
? Client ID: app_abc123...
? Client Secret: [hidden]
Configuration saved.
```

### 4. Client: Getting a Raw Bearer Token

For developers who need to interact with the gRPC API directly using tools like `grpcurl`, the `anvil auth get-token` command is a convenient way to get a valid JSON Web Token (JWT).

```bash
# This command uses the credentials from the currently active profile
anvil auth get-token
```
This will print a long token string to standard output, which can then be used in the `authorization` metadata header of a gRPC request (e.g., `authorization: Bearer <token>`).

```
```