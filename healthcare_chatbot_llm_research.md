# Wearable Health Chatbots: How Companies Feed Health Data to LLMs

*Research note — 2026-11-17*

## 1. The Problem

A health chatbot built on multi-vendor wearable data (WHOOP, Oura, Apple Watch, Polar…) must answer questions like:

- "When is my next event?" (lookup)
- "Suggest exercises based on my recent data" (coaching)
- "Show the ups and downs of my last 6 months" (trend analysis)

Passing raw data to the LLM fails for two reasons:

1. **Context bloat.** Six months of minute-level heart rate is about 260K data points.
2. **Weak numeric reasoning.** Google's PHIA study measured how well an LLM answers objective questions when it reads the raw numbers directly. Accuracy was **22%**. With code generation it was **74%**, and with an agent that plans, writes code and searches the web it was **84%**.

---

## 2. Company-by-Company

### Google: Fitbit Personal Health Coach, PHIA, PHA, PH-LLM
**Disclosure: High** (research blogs + peer-reviewed papers)

- **Architecture:** multi-agent system built on Gemini.
  - A **conversation agent** handles the dialogue, works out what the user wants, gathers context and coordinates the other agents.
  - A **data science agent** "iteratively uses tools to fetch, analyze, and summarize relevant data" and writes code when needed.
  - A **domain expert agent** (e.g. fitness) builds and adapts plans, grounded in authoritative sources.
  - A **health coach agent** (in the PHA research prototype) handles goal-setting and behavior change.
- **How they solved it:**
  - The data science agent works in **two stages**. It first turns a vague question into a **statistical analysis plan**, then **writes and runs code**. The LLM never does the arithmetic itself.
  - Results are compared against the **user's own baseline and population statistics**.
  - PH-LLM was fine-tuned on **daily aggregated** sensor data, not raw streams.
  - Evaluation used the SHARP framework (safety, helpfulness, accuracy, relevance, personalization). It involved more than 1M human annotations, and 173 unit tests checked the analysis code.

### WHOOP: WHOOP Coach (OpenAI GPT-4)
**Disclosure: Medium**

- **Architecture:** GPT-4 is combined with WHOOP's **own algorithms and a custom ML model**, plus the member's biometric data and Journal entries.
- **How they solved it:**
  - **Algorithms compute first, the LLM explains.** Recovery, Strain and Sleep scores are calculated before the LLM sees anything.
  - Data is reportedly **anonymized** before it goes to the LLM, under a zero-retention, zero-training policy.
  - **My Memory** (May 2026): a memory of personal context that members can view and edit.
  - **Proactive Check-Ins**: push recommendations triggered by the member's context.

### Strava: Athlete Intelligence (Claude Haiku on Amazon Bedrock)
**Disclosure: Medium** (AWS case study)

- **Architecture:** Claude Haiku via Bedrock analyzes workout stats (pace, heart rate, distance) against the athlete's past activities and their own description of the workout.
- **How they solved it:**
  - **Insights are generated after each upload**, so nothing waits on a chat request.
  - **Small, fast model**, chosen after testing on cost, speed and quality. Peak load is **14M tokens per minute** across 150M+ users.
  - The analysis window is limited, e.g. **30 days** for relative effort.
  - **Bedrock Guardrails** filter factual errors and harmful content.

### Oura: Oura Advisor
**Disclosure: Low** (press coverage only)

- **Architecture:** a chat layer on top of Oura's existing health algorithms.
- **How they solved it:**
  - **Memories** that users can view and delete.
  - **Check-ins** at a frequency the user sets.
  - In March 2026, Oura added its **own women's-health LLM** (built with webAI), hosted on infrastructure Oura controls.

### OpenAI: ChatGPT Health (January 2026)
**Disclosure: Medium**

- **Architecture:** connectors to Apple Health, MyFitnessPal, Peloton, Function and medical records. Wearable data only arrives through Apple Health.
- **How they solved it / lessons:**
  - **Proprietary scores (e.g. WHOOP Recovery) may not carry over.** Keep vendor-specific scores labeled by source.
  - Memories are not created from connected health data.
  - Health data is not used for model training.

### Garmin: Connect+ Active Intelligence
**Disclosure: Low**

- Users report insights that look like **templates with their numbers swapped in**, and reviews have been poor.
- **Lesson:** an LLM on top of a thin analysis layer produces generic answers.

### Ultrahuman: Jade
**Disclosure: Low**

- Chat over ring, CGM and blood data, plus the ability to take actions such as starting breathwork.
- **Explicit safety limits:** it does not diagnose and does not recognize emergencies.

### Apple: Project Mulberry
- According to Bloomberg, the standalone AI coach was **scaled back in February 2026**. Some features are being folded into the Health app.

### Uber: QueryGPT (not health, but the same problem of answering questions from a data warehouse)
**Disclosure: High**

- **Architecture:** an **intent agent** picks the business domain, a **table agent** picks the tables, and a **column-prune agent** cuts the schema down.
- **How they solved it:** full table schemas cost **40–60K tokens per request**. Pruning to what the question needs cut cost and response time. A golden set of questions with known correct answers drives evaluation.

---

## 3. The Common Pattern

None of the systems that work well pass raw data to the LLM. They split the work:

| Layer | Responsibility |
|---|---|
| **Code / algorithms** | All maths: scores, baselines, trends, statistics |
| **LLM** | Understanding intent, planning the analysis, choosing tools, explaining the result |
| **Memory** | A small user profile the user can edit (goals, injuries, context), always in the prompt |
| **Precompute / batch** | Common insights generated ahead of time; live multi-step analysis only for unusual questions |

Anthropic's context engineering guide states the same principle: keep *"the smallest set of high-signal tokens"* in context and load the rest *just in time* through tools.

---

## 4. Recommended Architecture

```
 Whoop / Oura / Apple / Polar APIs
            │
 [1] INGEST + NORMALIZE ──► canonical time-series store (tagged by source)
            │
 [2] PRECOMPUTE "Health Facts" (nightly + on new data)
     daily/weekly/monthly rollups · personal baselines
     trend slopes · anomalies · change-points · event calendar
            │
 [3] METRICS API / SEMANTIC LAYER (typed tools the LLM may call)
            │
 [4] ROUTER (small model) ──► picks the path for the question
            │
    ┌───────┼──────────────┬──────────────────┐
  Lookup  Insight        Coaching         Deep analysis
 (events) (6-mo trends)  (data snapshot + (agent writes code
                          expert RAG)      in a sandbox)
            │
 [5] CONTEXT: user profile card + memory + small JSON tool results
            │
 [6] ANSWER LLM ──► [7] VALIDATOR (every number traced to a tool result) + safety guardrails
```

**Key decisions**

1. **Normalize the data, but keep vendor scores separate.** WHOOP Recovery and Oura Readiness use different algorithms and are not interchangeable.
2. **Precompute a "Health Facts" layer.** This is the largest quality improvement. The same six months shrink from about 260K raw points to about 180 daily rows per metric, and to roughly 50 numbers once summarized into trends and anomalies.
3. **Put typed tools in front of the data instead of free-form SQL.** Examples: `get_metric_summary`, `compare_periods`, `get_trends`, `get_upcoming_events`, `get_baseline`. Each returns small JSON with the numbers already computed and flags such as `below_baseline`.
4. **Route by intent first** (Uber's pattern). Cheap questions stay cheap.
5. **Use a sandboxed code-execution agent** for questions nobody anticipated (Google's PHIA/PHA pattern).
6. **Ground every number.** Each figure in the answer must come from a tool result. Add medical-safety guardrails.

**Cost and response time at scale**

| Technique | Precedent |
|---|---|
| Model tiering: small model for routing and routine insights, large model only for deep analysis | Strava (Haiku) |
| Generate insights ahead of time (post-workout, daily, weekly) with off-peak batch jobs | Strava, WHOOP, Oura |
| Cache insights per user per day | General practice |
| Prompt caching of the system prompt, tool definitions and profile card | LLM provider feature |
| Send only the metrics and period the question needs | Uber |
| Call tools in parallel and stream responses | General practice |
| Golden question set, unit tests for analysis code, SHARP-style rubric | Uber, Google |

---

## 5. Sources

**Google**
- [How we are building the personal health coach](https://research.google/blog/how-we-are-building-the-personal-health-coach/)
- [The anatomy of a personal health agent](https://research.google/blog/the-anatomy-of-a-personal-health-agent/)
- [PHA paper — arXiv 2508.20148](https://arxiv.org/pdf/2508.20148v1.pdf)
- [PHIA paper — arXiv 2406.06464](https://arxiv.org/abs/2406.06464)
- [PH-LLM — Nature Medicine](https://research.google/pubs/a-personal-health-large-language-model-for-sleep-and-fitness-coaching/) · [preprint](https://arxiv.org/pdf/2406.06474)
- [ZenML LLMOps writeup on the Fitbit coach](https://www.zenml.io/llmops-database/ai-powered-personal-health-coach-using-gemini-models)

**WHOOP**
- [OpenAI case study](https://openai.com/index/whoop)
- [WHOOP Coach launch](https://www.whoop.com/us/en/press-center/whoop-unveils-the-new-whoop-coach-powered-by-openai/)
- [9to5Google on anonymization](https://9to5google.com/?p=588040)
- [My Memory & Proactive Check-Ins (May 2026)](https://www.businesswire.com/news/home/20260508464188/en)

**Strava**
- [AWS case study](https://aws.amazon.com/solutions/case-studies/strava-case-study/)
- [Android Central interview with Strava execs](https://www.androidcentral.com/apps-software/strava-interview-athlete-intelligence-inaccurate-gps-race-leaderboards-and-more)

**Oura**
- [Oura Advisor blog](https://ouraring.com/blog/oura-advisor)
- [Advisor rollout press release](https://www.businesswire.com/news/home/20250331565896/en/)
- [Oura women's-health LLM](https://www.t3.com/active/fitness-trackers/oura-smart-ring-womens-health-llm-advisor)

**OpenAI — ChatGPT Health**
- [Fortune](https://fortune.com/2026/01/07/openai-launches-chatgpt-health-in-a-push-to-become-a-hub-for-personal-health-data/)
- [Digital Trends](https://www.digitaltrends.com/computing/chatgpt-can-now-connect-to-your-apple-health-data-to-give-more-personalized-health-answers/)

**Garmin / Ultrahuman / Apple**
- [TechRadar — Garmin Active Intelligence](https://techradar.com/health-fitness/smartwatches/garmins-new-subscription-ai-feature-is-hilariously-bad-so-far)
- [Running with Rock — Garmin AI](https://runningwithrock.com/garmin-ai-possibilities/)
- [Ultrahuman Jade explainer](https://www.ultrahuman.com/science/explainers/jade/)
- [9to5Mac — Apple Mulberry scale-back](https://9to5mac.com/2026/02/05/apple-reportedly-scales-back-plans-for-ai-powered-health-coach/)

**Architecture patterns**
- [Uber QueryGPT](https://www.uber.com/blog/query-gpt)
- [dbt — Semantic layer vs text-to-SQL (2026)](https://docs.getdbt.com/blog/semantic-layer-vs-text-to-sql-2026)
- [Cube — Semantic layers for AI](https://cube.dev/blog/semantic-layers-the-missing-piece-for-ai-enabled-analytics)
- [Anthropic — Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)

### Confidence notes
- **Google** is the only company that has published detailed architecture.
- **WHOOP and Strava** are partly documented through vendor case studies.
- **Oura, Garmin and Ultrahuman** have disclosed little; their sections rely mostly on press coverage.
- **Apple's** plans come from Bloomberg's reporting; Apple did not comment.
- The dbt and Cube semantic-layer accuracy figures are vendor-run on small test sets.
