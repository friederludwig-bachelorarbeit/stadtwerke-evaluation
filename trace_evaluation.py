import json
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict


def span_duration_distribution_by_service(file_path):
    try:
        # JSON-Datei einlesen
        with open(file_path, 'r') as file:
            data = json.load(file)

        service_durations = defaultdict(list)

        # Über alle Traces iterieren
        for trace in data.get('data', []):
            for span in trace.get('spans', []):
                # Extrahiere ServiceName und Dauer des Spans
                service_name = span.get('process', {}).get(
                    'serviceName', 'Unbekannt')
                duration = span.get('duration')
                if duration is not None:
                    service_durations[service_name].append(
                        duration / 1000.0)  # In Millisekunden

        # Erstelle Histogramme für jeden Service
        for service, durations in service_durations.items():
            plt.figure(figsize=(10, 5))
            plt.hist(durations, bins=30, alpha=0.7, label=f"{service}")
            plt.title(f"Verteilung der Span-Durchlaufzeiten für {service}")
            plt.xlabel("Dauer (in Millisekunden)")
            plt.ylabel("Häufigkeit")
            plt.legend()
            plt.show()

    except Exception as e:
        print(f"Fehler beim Verarbeiten der Datei: {e}")


def calculate_trace_durations_with_plots(file_path):
    try:
        # JSON-Datei einlesen
        with open(file_path, 'r') as file:
            data = json.load(file)

        trace_durations = []
        trace_count = len(data.get('data', []))

        # Über alle Traces iterieren
        for trace in data.get('data', []):
            if 'spans' in trace:
                # Die Dauer des gesamten Traces berechnen (basierend auf Startzeit und Endzeit der Spans)
                start_times = [span.get('startTime')
                               for span in trace['spans'] if 'startTime' in span]
                end_times = [span.get('startTime', 0) + span.get('duration', 0)
                             for span in trace['spans'] if 'startTime' in span and 'duration' in span]

                if start_times and end_times:
                    # In Millisekunden
                    trace_duration = (
                        max(end_times) - min(start_times)) / 1000.0
                    trace_durations.append(trace_duration)

        if not trace_durations:
            print("Keine Traces mit berechenbarer Dauer gefunden.")
            return

        # Durchschnittliche Dauer mit Ausreißern berechnen
        average_trace_duration_with_outliers = sum(
            trace_durations) / len(trace_durations)

        # Ausreißer mit der IQR-Methode entfernen
        q1 = np.percentile(trace_durations, 25)
        q3 = np.percentile(trace_durations, 75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Filtere die Daten
        filtered_trace_durations = [
            d for d in trace_durations if lower_bound <= d <= upper_bound]

        # Durchschnitt berechnen (nach dem Entfernen von Ausreißern)
        if not filtered_trace_durations:
            print("Keine Daten übrig nach Entfernung der Ausreißer.")
            return

        average_trace_duration_without_outliers = sum(
            filtered_trace_durations) / len(filtered_trace_durations)

        print(f"Anzahl der Traces: {trace_count}")
        print(
            f"Anzahl der ursprünglichen Trace-Durchlaufzeiten: {len(trace_durations)}")
        print(
            f"Anzahl der Trace-Durchlaufzeiten nach Entfernung von Ausreißern: {len(filtered_trace_durations)}")
        print(
            f"Durchschnittliche Trace-Durchlaufzeit mit Ausreißern: {average_trace_duration_with_outliers:.2f} Millisekunden")
        print(
            f"Durchschnittliche Trace-Durchlaufzeit ohne Ausreißer: {average_trace_duration_without_outliers:.2f} Millisekunden")

        # Schaubilder erstellen
        # 1. Boxplot der Trace-Durchlaufzeiten
        plt.figure(figsize=(10, 5))
        plt.boxplot(trace_durations, vert=False, patch_artist=True)
        plt.title("")
        plt.xlabel("Dauer (in Millisekunden)")
        plt.show()

        # 2. Histogramm vor und nach Ausreißerentfernung
        plt.figure(figsize=(10, 5))
        plt.hist(trace_durations, bins=30, alpha=0.7, label="Originaldaten")
        plt.hist(filtered_trace_durations, bins=30,
                 alpha=0.7, label="Nach Ausreißerentfernung")
        plt.title("Histogramm der Trace-Durchlaufzeiten")
        plt.xlabel("Dauer (in Millisekunden)")
        plt.ylabel("Häufigkeit")
        plt.legend()
        plt.show()

    except Exception as e:
        print(f"Fehler beim Verarbeiten der Datei: {e}")


# Pfad zur hochgeladenen Datei
file_path = "data/1S_10_25.json"
calculate_trace_durations_with_plots(file_path)
# span_duration_distribution_by_service(file_path)
