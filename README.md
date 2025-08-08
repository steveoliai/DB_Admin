# Multi-Tenant Architecture: A Database Per Tenant?  
_A DB-Native Approach to Migration Management_

## Overview
When designing a **multi-tenant architecture**, your choice of strategy depends on factors like **tenant isolation**, **scalability**, **operational complexity**, **security**, and **cost**.  
Here’s a simplified trade-off spectrum:

- **Highest isolation & control** → One database per tenant  
- **Balance of isolation & manageability** → One schema per tenant  
- **Simplicity & scalability** → Shared schema with partitioned tables  

If you choose **a separate database per tenant**, you must manage the operational overhead of maintaining and migrating objects across many databases.

While tools like **Flyway** and **Liquibase** work well, this proof of concept uses a **database-native approach** for more granular control — particularly for:
- Table creation (including **partitioning** support)
- Managing objects like **stored procedures**

The implementation runs entirely **inside PostgreSQL** using the `dblink` extension, with authentication currently handled in stored procedures (to be reworked for production).

---

## Architecture Diagram

```mermaid
flowchart TB
  subgraph AdminDB["Admin DB - postgres"]
    A["Schema: admmgt"]
    A1["Migration Metadata<br/>(vendor_db_settings, scripts, etc.)"]
    A2["Stored Procedures"]
  end

  subgraph TemplateDB["Template DB - unitemplate"]
    T1["Schema: mgttest"]
    T2["Base Tables & Structures"]
    T3["Stored Procedures"]
  end

  subgraph Tenants["Multiple Tenant Databases"]
    direction TB
    DB1["Tenant DB - BigClient"]
    DB2["Tenant DB - ClientB"]
    DB3["Tenant DB - ClientC"]
  end

  A -- "dblink" --> TemplateDB
  A -- "dblink" --> DB1
  A -- "dblink" --> DB2
  A -- "dblink" --> DB3

  TemplateDB -- "Structure Copy" --> DB1
  TemplateDB -- "Structure Copy" --> DB2
  TemplateDB -- "Structure Copy" --> DB3
