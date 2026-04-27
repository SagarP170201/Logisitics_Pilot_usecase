# Logistics AI Pilot — Snowflake Implementation

## What Are We Doing?

This pilot implements **two AI use cases** for a logistics customer using **Snowflake-native** features — no external infra needed. Everything runs inside Snowflake.

### Use Case 1: POD Document Processing
**Problem:** Proof of Delivery (POD) documents arrive as PDFs. Today, extracting structured data (who signed, was there damage, which shipment) is manual.

**Solution:**
- POD PDFs land in a Snowflake stage (internal or S3)
- `AI_PARSE_DOCUMENT` (LAYOUT mode) extracts the full text from each PDF
- `AI_EXTRACT` pulls structured fields (shipment ID, signed by, exceptions, damage) into a curated table
- A **Cortex Search Service** indexes all POD text so you can semantically search across documents (e.g., "find all PODs with water damage for Acme Corp")
- Streams + Tasks automate this end-to-end — new PDFs are processed automatically

### Use Case 3: Conversational Agent ("Talk to Your Data")
**Problem:** Operations teams need to ask ad-hoc questions across shipments, orders, PODs, claims, and carriers — today this requires writing SQL or waiting for BI reports.

**Solution:**
- A **Semantic View** defines a business-friendly data model over 6 logistics tables (shipments, orders, PODs, claims, customers, carriers) with pre-defined metrics (on-time %, exception rate, claim amounts, etc.)
- A **Cortex Agent** combines two tools:
  - **Tool 1 — Cortex Analyst** (text-to-SQL): Answers KPI/analytics questions by generating SQL against the semantic view
  - **Tool 2 — Cortex Search**: Finds specific POD documents by semantic search
- Users ask natural language questions; the agent routes to the right tool, gets the answer, and responds

### Use Case 2: Call Center Agent (NOT in this pilot)
Requires architectural discussion with Shiva (real-time conversation, channel integration, auth, latency SLA).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     AI_PLATFORM_DEV                             │
│                                                                 │
│  ┌──────────────┐   ┌───────────────┐   ┌───────────────────┐  │
│  │   RAW        │   │   CURATED     │   │   SEMANTIC        │  │
│  │              │   │               │   │                   │  │
│  │ POD_STAGE    │──>│ FACT_SHIPMENTS│   │ LOGISTICS_OPS_    │  │
│  │ POD_FILES    │   │ FACT_ORDERS   │──>│ ANALYTICS         │  │
│  │ POD_EXTRACTED│──>│ POD_FACT      │   │ (Semantic View)   │  │
│  │              │   │ FACT_CLAIMS   │   │                   │  │
│  │ Streams +    │   │ DIM_CUSTOMERS │   │ POD_SEARCH_       │  │
│  │ Tasks        │   │ DIM_CARRIERS  │──>│ SERVICE           │  │
│  │ (AI_PARSE +  │   │               │   │ (Cortex Search)   │  │
│  │  AI_EXTRACT) │   │               │   │                   │  │
│  └──────────────┘   └───────────────┘   │ LOGISTICS_OPS_    │  │
│                                         │ AGENT             │  │
│                                         │ (Cortex Agent)    │  │
│                                         └───────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Data flow:**
1. POD PDFs → `RAW.POD_STAGE` → `AI_PARSE_DOCUMENT` → `AI_EXTRACT` → `CURATED.POD_FACT`
2. All curated tables → `SEMANTIC.LOGISTICS_OPS_ANALYTICS` (Semantic View for Cortex Analyst)
3. POD_FACT + dimensions → `SEMANTIC.POD_SEARCH_SERVICE` (Cortex Search)
4. Agent wires both tools together for natural language access

---

## Scripts (Run in Order)

| # | Script | What It Does |
|---|--------|--------------|
| 1 | `scripts/01_landing_zone.sql` | Creates database, schemas, stage, warehouse |
| 2 | `scripts/02_dimension_tables.sql` | Creates DIM_CARRIERS, DIM_CUSTOMERS with sample data |
| 3 | `scripts/03_fact_tables.sql` | Creates FACT_SHIPMENTS, FACT_ORDERS, POD_FACT, FACT_CLAIMS with sample data |
| 4 | `scripts/04_pod_ingestion_pipeline.sql` | Stream + Task pipeline for AI_PARSE_DOCUMENT + AI_EXTRACT |
| 5 | `scripts/05_cortex_search_service.sql` | Search corpus table + Cortex Search Service |
| 6 | `scripts/06_semantic_view.sql` | Semantic view via SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML |
| 7 | `scripts/07_cortex_agent.sql` | Cortex Agent with Analyst + Search tools |
| 8 | `scripts/08_validation.sql` | Verification queries and test cases |

---

## How to Run

1. Open each script in **Snowsight** (or any SQL client connected to your Snowflake account)
2. Run them **in order** (01 through 08)
3. Use `ACCOUNTADMIN` role (or a role with CREATE DATABASE, CREATE AGENT, CORTEX_USER privileges)
4. After script 07, test the agent via **Snowflake Intelligence** or the Cortex Agent REST API

---

## Customization for Your Environment

- **Replace sample data** in scripts 02 and 03 with your actual logistics tables
- **POD Stage:** For production, change `POD_STAGE` from internal to an external stage pointing to your S3/ADLS/GCS bucket
- **Warehouse sizing:** `AI_WH` is XS for DEV. Scale up for production volumes
- **Semantic view:** Add more metrics, dimensions, and verified queries (VQRs) as you learn what users ask
- **Agent instructions:** Tune the orchestration and response instructions based on testing feedback

---

## Key Snowflake Features Used

| Feature | Purpose |
|---------|---------|
| `AI_PARSE_DOCUMENT` | Extract text from POD PDFs (OCR/LAYOUT) |
| `AI_EXTRACT` | Pull structured fields from unstructured text |
| Streams + Tasks | Automate the ingestion pipeline |
| Cortex Search Service | Semantic search over POD documents |
| Semantic View | Business-friendly data model for text-to-SQL |
| Cortex Analyst | Natural language → SQL generation |
| Cortex Agent | Orchestrates multiple tools for conversational AI |

---

## Production Checklist

See `LOGISTICS_AI_IMPLEMENTATION_GUIDE.md` for the full step-by-step guide including production promotion checklist.
