# CI/CD Integration Considerations  
_For Multi-Tenant Database-Per-Tenant Migration Framework_

## Overview
This document outlines strategies and considerations for integrating the **`admmgt`**-based migration management framework into a CI/CD pipeline.  
The goal is to ensure that **schema changes**, **stored procedures**, and **partition management** are applied consistently and safely across:

- The **Template Database** (`unitemplate`)
- All **Tenant Databases** registered in `admmgt.vendor_db_settings`

---

## Pipeline Stages

### 1. **Pre-Deployment (Build)**
- **Validate Scripts**  
  - Ensure new migration scripts are **syntactically valid** SQL.
  - Run linting (e.g., `pg_format`, `sqlfluff`).
- **Unit Test Stored Procedures**  
  - Execute procedures in an isolated test DB.
- **Versioning**  
  - Assign unique `script_id` values in `admmgt.scripts`.
  - Update `scriptversion` in metadata only after successful validation.

---

### 2. **Deployment (Apply Changes)**
1. **Connect to Admin DB**
   - Pipeline authenticates to the **Admin DB** using a **secure secret store**:
     - HashiCorp Vault  
     - AWS Secrets Manager  
     - GCP Secret Manager  
     - Azure Key Vault  
2. **Insert Script Metadata**
   - Populate:
     - `admmgt.scripts`
     - `admmgt.script_tables`
     - `admmgt.script_table_columns`
     - `admmgt.script_table_partitions` (if applicable)
3. **Mark Script Ready**
   - Set `readyflag = true` or equivalent status.
4. **Run Migration Command**
   ```sql
   CALL admmgt.applyScripts();
   ```

### 3. **Post-Deployment (Maintenance & QA)**
   - **Partition Maintenance**       
    ```sql        
   CALL admmgt.applyMaintenance(t_numdays => 30);    
   ```   
   - **Procedure Sync**       
   ```sql       
   CALL admmgt.refesh_stored_procedures();
   ```
   - **Smoke Tests across sampled tenants.**
   - **Centralized Logging (ELK, Cloud Logging, CloudWatch, etc.).**

### 4. **Rollback Strategy**
   - **Procedural Rollbacks: Ship inverse procedures with the migration.**
   - **Transactional Safety: Wrap changes in transactions where feasible.**
   - **Template-First Rollback: Revert unitemplate prior to tenant DBs.**
   - **Targeted Rollback: Control blast radius using updateflag.**
---

## CI/CD Flow Diagram

```mermaid
flowchart LR
  A[Developer commits SQL changes and migration metadata] --> B[CI triggers on changes under the migrations path]
  B --> C[Validate SQL with linters and run unit tests for procedures]
  C --> D[Connect to Admin DB using secrets from a vault]
  D --> E[Insert or update metadata in admmgt tables: scripts, script_tables, columns, partitions]
  E --> F[Mark script ready for apply]
  F --> G[Run CALL admmgt.applyScripts]
  G --> H{Apply to Template DB successful}
  H -- Yes --> I[Propagate to tenant databases with updateflag true via dblink]
  I --> J[Update scriptversion for each database and capture results]
  J --> K[Optional maintenance: CALL admmgt.applyMaintenance with lookahead days]
  K --> L[Sync procedures to tenants: CALL admmgt.refesh_stored_procedures]
  L --> M[Publish logs and status to CI artifacts and observability]
  H -- No --> R[Fail job, publish errors, halt propagation]