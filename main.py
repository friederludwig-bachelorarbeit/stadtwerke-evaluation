from trace_evaluation import merge_and_calculate_trace_durations

# =============================================================================
# 1 Nachricht/Sekunde | v1 ✅ | v2 ✅ | v3 ✅
# =============================================================================
filepaths_1 = ["data/1/v3/mqtt-consumer_traces.jsonl",
               "data/1/v3/validation-service_traces.jsonl",
               "data/1/v3/persistence-service_traces.jsonl"]
# traces_1 = merge_and_calculate_trace_durations(filepaths_1, 30)

# print(traces_1)


# =============================================================================
# 10 Nachrichten/Sekunde | v1 ✅ | v2 ✅ | v3 ✅
# =============================================================================
filepaths_10 = ["data/10/v3/mqtt-consumer_traces.jsonl",
                "data/10/v3/validation-service_traces.jsonl",
                "data/10/v3/persistence-service_traces.jsonl"]
# traces_10 = merge_and_calculate_trace_durations(filepaths_10, 30)

# print(traces_10)

# =============================================================================
# 50 Nachrichten/Sekunde | v1 ✅ | v2 ✅ | v3 ✅
# =============================================================================
filepaths_50 = ["data/50/v3/mqtt-consumer_traces.jsonl",
                "data/50/v3/validation-service_traces.jsonl",
                "data/50/v3/persistence-service_traces.jsonl"]
df_trace_durations_50 = merge_and_calculate_trace_durations(filepaths_50)

print(df_trace_durations_50)
