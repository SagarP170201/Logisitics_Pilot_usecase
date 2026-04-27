/*
 * Script 07: Cortex Agent
 * Creates: LOGISTICS_OPS_AGENT with Cortex Analyst + Cortex Search tools
 * Test via Snowflake Intelligence UI or Cortex Agent REST API
 */

CREATE OR REPLACE AGENT AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_AGENT
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 900,
      "tokens": 400000
    }
  },
  "instructions": {
    "orchestration": "You are LogisticsOps Assistant, a specialized logistics analytics agent for operations teams.\n\nYour Scope: Answer questions about shipments, orders, POD (Proof of Delivery) documents, claims, carriers, and customers.\n\nTool Selection Guidelines:\n- For KPI/analytics questions (on-time rates, costs, volumes, trends, comparisons): Use logistics_analytics.\n- For searching specific POD documents, exception details, damage descriptions: Use pod_search.\n- If a question needs both structured data AND document lookup, use both tools.\n\nBusiness Context:\n- POD = Proof of Delivery\n- Exception = any issue during delivery (damage, partial delivery, refused, missing signature)\n- On-time = delivered by the expected delivery date\n- Claims are filed by customers against carriers for damage, loss, shortage, or late delivery\n\nBoundaries:\n- You do NOT have real-time tracking data.\n- You cannot modify shipments, orders, or claims. Analytics and search only.\n- For production deployment architecture, tell the user to discuss with Shiva.",
    "response": "Be concise and professional. Lead with the direct answer, then supporting details. Use tables for multi-row results. Always include the time period and data scope. When showing rates, also show absolute numbers."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "logistics_analytics",
        "description": "Queries structured logistics data for KPI and analytics questions about shipments, orders, POD documents, claims, carriers, and customers. Key metrics: on_time_delivery_rate, exception_rate, damage_rate, missing_signature_count, total_freight_cost, total_claim_amount. Use for aggregated KPIs, trends, comparisons. Do NOT use for searching specific POD document text (use pod_search)."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "pod_search",
        "description": "Searches POD (Proof of Delivery) document content using semantic search. Finds specific POD records by text content, exception notes, damage descriptions, and metadata (POD_ID, SHIPMENT_ID, CUSTOMER_NAME, CARRIER_NAME, POD_STATUS). Use for finding specific PODs, exception details, damage descriptions. Do NOT use for aggregated analytics (use logistics_analytics)."
      }
    }
  ],
  "tool_resources": {
    "logistics_analytics": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "AI_WH"
      },
      "semantic_view": "AI_PLATFORM_DEV.SEMANTIC.LOGISTICS_OPS_ANALYTICS"
    },
    "pod_search": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "AI_WH"
      },
      "search_service": "AI_PLATFORM_DEV.SEMANTIC.POD_SEARCH_SERVICE"
    }
  }
}
$$;
