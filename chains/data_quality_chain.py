# chains/data_quality_chain.py
from nodes.collect_dq_dimensions_step import collect_dq_dimensions_step
from nodes.ai_detect_dq_anomalies_step import ai_detect_dq_anomalies_step
from nodes.ai_update_dq_memory_step import ai_update_dq_memory_step
from nodes.dq_summary_step import dq_summary_step
from langgraph.graph import END

def data_quality_chain(graph):
    # --- Add DQ nodes ---
    graph.add_node("collect_dq_dimensions_step", collect_dq_dimensions_step)
    graph.add_node("ai_detect_dq_anomalies_step", ai_detect_dq_anomalies_step)
    graph.add_node("ai_update_dq_memory_step", ai_update_dq_memory_step)
    graph.add_node("dq_summary_step", dq_summary_step)

    # --- Define edges for DQ flow ---
    graph.add_edge("collect_dq_dimensions_step", "ai_detect_dq_anomalies_step")
    graph.add_edge("ai_detect_dq_anomalies_step", "ai_update_dq_memory_step")
    graph.add_edge("ai_update_dq_memory_step", "dq_summary_step")
    graph.add_edge("dq_summary_step", END)

    return graph
