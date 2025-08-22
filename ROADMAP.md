# 🚧 Roadmap

This document outlines planned features and enhancements for the **Postgres Tenant Admin (admmgt)** framework.  
The order below does not necessarily reflect priority - features may be developed in parallel.

---

## ✨ Planned Features

### 1. ALTER COLUMN Support
Enable migrations that can alter existing column definitions (e.g., data type changes, nullability updates, default changes) while tracking applied changes in the metadata.

### 2. DROP COLUMN Support
Provide safe handling for column removal, including:
- Warnings for destructive changes
- Rollback procedures to recreate dropped columns if possible

### 3. VIEW Support
Add the ability to manage and version SQL views:
- Create and alter views as part of migration scripts
- Include dependencies in metadata for replay/rollback support

### 4. REPLAY (Targeted Upgrades)
Introduce the ability to **replay scripts** so that tenant databases that lag behind the template can be upgraded to a **selected target script version**:
- Useful for tenants that elect not to receive every update immediately
- Provides controlled catch-up to a known version

### 5. ROLLBACK Enhancements
Expand current rollback design to:
- Support paired forward/rollback procedures for each script
- Allow rollback only where a given script has been applied
- Record rollback attempts and outcomes

### 6. Data Life Cycle Management
Automate common lifecycle operations such as:
- Partition rotation and archival
- Expiring/deleting old data by policy

### 7. Trigger Support
Allow migration scripts to define and manage **database triggers**, including:
- Creation and alteration of trigger functions
- Tracking trigger definitions across tenants
- Safe rollback/drop when required

### 8. Multi-Threaded Execution
Support parallel rollout of scripts to multiple tenant databases by:
- Running apply/rollback across multiple sessions/threads
- Improving overall performance in large-scale environments

---

## 🗺️ Future Considerations
- Dry-run / plan mode to preview changes before execution
- Schema drift detection and reporting
- Optional monitoring dashboard over audit tables


