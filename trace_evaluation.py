import pandas as pd
import json


def load_traces_from_file(filepath):
    data = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line.strip()))
        return pd.DataFrame(data)
    except FileNotFoundError:
        print(f"Datei {filepath} wurde nicht gefunden.")
        return pd.DataFrame()
    except json.JSONDecodeError as e:
        print(f"Fehler beim lesen der Datei {filepath}: {e}")
        return pd.DataFrame()


def merge_and_calculate_trace_durations(filepaths, time_window_minutes=None):
    """
    Kombiniert die Traces aus mehreren Dateien, berechnet die Gesamtdurchlaufzeit pro Trace,
    filtert optional nach einer Zeitspanne und berechnet den Mittelwert mit und ohne Ausreißer.

    :param filepaths: Liste von Datei-Pfaden zu den Trace-Dateien
    :param time_window_minutes: Zeitspanne in Minuten (ab Startzeit des ersten Traces), optional
    :return: dict mit durchschnittlicher Durchlaufzeit, Anzahl der Traces,
             durchschnittlicher Durchlaufzeit ohne Ausreißer und Anzahl der Traces ohne Ausreißer.
    """
    # Lade alle Dateien in separate DataFrames
    dfs = [load_traces_from_file(filepath) for filepath in filepaths]

    # Kombiniere alle DataFrames in einen
    combined_df = pd.concat(dfs, ignore_index=True)

    # Konvertiere Start- und Endzeitspalten zu numerischen Werten
    combined_df["startTimeUnixNano"] = pd.to_numeric(
        combined_df["startTimeUnixNano"], errors="coerce")
    combined_df["endTimeUnixNano"] = pd.to_numeric(
        combined_df["endTimeUnixNano"], errors="coerce")

    # Fehlende Werte entfernen
    combined_df = combined_df.dropna(
        subset=["startTimeUnixNano", "endTimeUnixNano"])

    # Gruppiere nach traceId und berechne die Gesamtdurchlaufzeit
    trace_durations = combined_df.groupby("traceId").apply(
        lambda group: {
            "start_time": group["startTimeUnixNano"].min(),
            "end_time": group["endTimeUnixNano"].max(),
            "duration_ns": group["endTimeUnixNano"].max() - group["startTimeUnixNano"].min()
        }
    )

    # Umwandeln in DataFrame
    result_df = pd.DataFrame([
        {"traceId": trace_id, **values}
        for trace_id, values in trace_durations.items()
    ])

    # Optional: Dauer in Millisekunden hinzufügen
    result_df["duration_ms"] = result_df["duration_ns"] / 1e6

    # Optional: Traces innerhalb der Zeitspanne filtern
    if time_window_minutes is not None:
        # Zeitfenster in Nanosekunden
        time_window_ns = time_window_minutes * 60 * 1e9

        # Früheste Startzeit des ersten Traces
        earliest_start_time = result_df["start_time"].min()

        # Filtere Traces, die innerhalb der Zeitspanne liegen
        result_df = result_df[
            result_df["start_time"] <= (earliest_start_time + time_window_ns)
        ]

    # Anzahl der Traces im Zeitfenster
    num_traces_in_window = len(result_df)

    # Berechne die durchschnittliche Durchlaufzeit
    average_duration_ms = round(result_df["duration_ms"].mean(), 2)

    # Berechne den Mittelwert ohne Ausreißer (IQR-Methode)
    q1 = result_df["duration_ms"].quantile(0.25)  # 1. Quartil
    q3 = result_df["duration_ms"].quantile(0.75)  # 3. Quartil
    iqr = q3 - q1  # Interquartilsabstand

    # Filter für Werte innerhalb von [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    filtered_durations = result_df[(result_df["duration_ms"] >= lower_bound) &
                                   (result_df["duration_ms"] <= upper_bound)]

    # Mittelwert ohne Ausreißer
    average_duration_no_outliers = round(
        filtered_durations["duration_ms"].mean(), 2)

    # Anzahl der Traces ohne Ausreißer
    num_traces_no_outliers = len(filtered_durations)

    return {
        "average_duration_ms": average_duration_ms,
        "num_traces_in_window": num_traces_in_window,
        "average_duration_no_outliers": average_duration_no_outliers,
        "num_traces_no_outliers": num_traces_no_outliers
    }
