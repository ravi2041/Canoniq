# 🧠 Analytics AI — Natural Language to SQL Agent

### Conversational Analytics Engine for Unified Marketing, Shopify, and GA4 Data

---

## 🚀 Overview
Analytics AI turns plain English questions into data-driven insights.

It uses **LangChain**, **LangGraph**, and **LLM reasoning** to convert natural language questions into **validated SQL queries**, execute them, summarize results, and even suggest the best visualizations.

---

## 🧩 Key Features
- 🧠 **Natural Language to SQL** – Ask “Top 10 products by revenue this year” and get instant SQL & results.  
- ⚙️ **Multi-Domain Router** – Automatically detects whether query relates to **Shopify**, **GA4**, or **Marketing**.  
- ✅ **Auto Validation & Fixes** – Detects broken SQL, fixes joins, normalizes syntax.  
- 📊 **Chart Suggestion Engine** – Suggests best chart types (bar, line, funnel, etc.) based on data.  
- 🗣️ **Narrative Summary** – Generates AI-written summaries and recommendations from query results.  
- 🔄 **Combined Source Analysis** – Merge Shopify (sales) with GA4 (web activity) for holistic insights.  

---

## 🧠 Architecture Overview

```mermaid
flowchart TD
    U[User / API / UI] --> A[run_langgraph_agent]

    A --> B[State Initialization]
    B --> B1[Load question + chat history]
    B --> B2[Load metadata]
    B --> B3[Generate run_id]
    B --> B4[Load long-term memory]

    B --> C[Keypoints Node]
    C --> C1[Extract metrics]
    C --> C2[Extract filters]
    C --> C3[Extract time window]
    C --> C4[Extract grouping/output hints]

    C --> D[Supervisor Router]
    D -->|marketing| E1[Marketing SQL Generator]
    D -->|shopify| E2[Shopify SQL Generator]
    D -->|ga4| E3[GA4 SQL Generator]
    D -->|combined| E4[Combined SQL Generator]
    D -->|data_quality| Q1[Data Quality Chain]

    E1 --> F[SQL Validator]
    E2 --> F
    E3 --> F
    E4 --> F

    F -->|valid| G[SQL Executor]
    F -->|invalid| H[Fix SQL Node]
    H --> F

    G -->|large result / checkpoint| I[Human Checkpoint]
    G -->|normal result| J[Summarizer]
    I --> J

    J --> J1[Narrative summary]
    J --> J2[Recommendations]
    J --> J3[Keyword extraction]
    J --> J4[Memory update + embeddings]

    J --> K[Chart Suggestion Node]
    K --> L[Final Structured Output]

    Q1 --> L

    subgraph Platform Services
        M1[Metadata files]
        M2[MySQL execution layer]
        M3[Observability + artifacts]
        M4[Memory store]
    end

    B2 --> M1
    G --> M2
    C --> M3
    F --> M3
    G --> M3
    J --> M4

```
# 🧠 Canoniq Architecture Overview (MCP-Based Design)

Canoniq is a **stateful, multi-domain analytics agent** built on **LangGraph**.  
Instead of connecting directly to databases, the system uses an **MCP (Model Context Protocol) tool layer** to access structured data, metadata, validation services, and other external resources.

This design decouples orchestration from data access and creates a more scalable, secure, and extensible architecture for analytics, governance, and future agentic workflows.

---

## 1. High-Level Architecture

```mermaid
flowchart TD
    U[User / API / UI] --> A[run_langgraph_agent]

    A --> B[State Initialization]
    B --> B1[Load user question]
    B --> B2[Load chat history]
    B --> B3[Load metadata context]
    B --> B4[Generate run_id]
    B --> B5[Load long-term memory]

    B --> C[Keypoints Extraction Node]
    C --> C1[Extract metrics]
    C --> C2[Extract filters]
    C --> C3[Extract dimensions]
    C --> C4[Extract date range]
    C --> C5[Extract output intent]

    C --> D[Supervisor Router]

    D -->|Marketing| E1[Marketing SQL Generator]
    D -->|Shopify| E2[Shopify SQL Generator]
    D -->|GA4| E3[GA4 SQL Generator]
    D -->|Combined| E4[Combined SQL Generator]
    D -->|Data Quality| Q1[Data Quality Chain]

    E1 --> F[SQL Validator]
    E2 --> F
    E3 --> F
    E4 --> F

    F -->|Valid| G[MCP Query Executor Node]
    F -->|Invalid| H[Fix SQL Node]
    H --> F

    G --> MCP[MCP Client Layer]

    MCP --> M1[MCP SQL Server]
    MCP --> M2[MCP Metadata Server]
    MCP --> M3[MCP Governance / Validation Server]
    MCP --> M4[MCP Memory / Context Server]
    MCP --> M5[MCP Logging / Artifact Server]

    M1 --> DS1[(Marketing DB)]
    M1 --> DS2[(Shopify DB)]
    M1 --> DS3[(GA4 DB)]
    M1 --> DS4[(Combined Analytics DB)]

    G -->|Large result / guardrail| I[Human Checkpoint]
    G -->|Normal result| J[Summarizer Node]
    I --> J

    J --> J1[Narrative Summary]
    J --> J2[Recommendations]
    J --> J3[Assumptions / Keywords]
    J --> J4[Memory Update]

    J --> K[Chart Suggestion Node]
    K --> L[Final Structured Output]

    Q1 --> MCP
    Q1 --> L
```
## MCP-Oriented Detailed Architecture

```mermaid
flowchart LR
    A[LangGraph Agent] --> B[Reasoning / Routing Layer]
    B --> C[SQL Generation Layer]
    C --> D[Validation / Repair Layer]
    D --> E[MCP Client Adapter]

    E --> F1[MCP SQL Tool]
    E --> F2[MCP Metadata Tool]
    E --> F3[MCP Governance Tool]
    E --> F4[MCP Memory Tool]
    E --> F5[MCP Artifact Tool]

    F1 --> G1[(Marketing DB)]
    F1 --> G2[(Shopify DB)]
    F1 --> G3[(GA4 DB)]
    F1 --> G4[(Combined DB)]

    F2 --> H1[Schema Registry]
    F2 --> H2[Business Glossary]
    F2 --> H3[Canonical Naming Rules]

    F3 --> I1[Anomaly Rules]
    F3 --> I2[Validation Rules]
    F3 --> I3[Data Quality Checks]

    F4 --> J1[Conversation Memory]
    F4 --> J2[Insight Memory]

    F5 --> K1[Logs]
    F5 --> K2[Artifacts]
    F5 --> K3[Execution Trace]

```
