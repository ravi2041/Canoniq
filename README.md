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


