"""
Agentic AI Root Cause Analysis System - Updated v6 (FIXED)
Peer-Metric Context Anomaly Detection with Gemini Intelligence
Ranked suspects (top 10) + Metrics-only handling + PNG generation
FIXED: Corrected Gemini API call
"""

import os
import sys
import json
import glob
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

# ============================================================================
# GEMINI API SETUP (FIXED)
# ============================================================================

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except:
    HAS_GEMINI = False

# Try to import IPython
try:
    from IPython.display import Image, display, SVG
    from IPython import get_ipython
    HAS_IPYTHON = True
except:
    HAS_IPYTHON = False

# ============================================================================
# MERMAID DIAGRAM CODE
# ============================================================================

MERMAID_DIAGRAM = """
graph TD
    START([START]) --> LD["Load Data<br/>KPI, Pod Metrics, Logs"]
    LD --> LS["Learn Dataset Structure<br/>Extract pods, metrics, context"]
    LS --> AK["Analyze KPI Files<br/>Compute statistics"]
    AK --> AC["Ask Gemini<br/>Select peer-metric context"]
    AC --> CA["Create Actions<br/>Plan 7 detection methods"]
    CA --> PA["Perform Action<br/>Execute anomaly detection"]
    PA --> AR["Analyze Result<br/>Collect & Rank anomalies"]
    AR --> CR{"Root Cause<br/>Found?"}
    CR -->|Yes| SUM["Summarize<br/>Identify affected pod & timestamp"]
    CR -->|No| CA
    SUM --> OUT["Output: Pod + Timestamp<br/>+ Ranked Suspects + Diagram"]
    OUT --> END([END])

    style START fill:#90EE90
    style END fill:#FFB6C1
    style LD fill:#87CEEB
    style LS fill:#87CEEB
    style AK fill:#87CEEB
    style AC fill:#FFD700
    style CA fill:#FFD700
    style PA fill:#FFD700
    style AR fill:#FFA07A
    style CR fill:#FFA07A
    style SUM fill:#98FB98
    style OUT fill:#98FB98
"""

# ============================================================================
# CONFIGURATION
# ============================================================================

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyA0AbJnQrxpDVtODqyfUyWmw2FPNXjS4WU")
GEMINI_MODEL = "gemini-2.5-flash"

DATA_ROOT = Path("preprocessed_data")
LOGS_FOLDER = DATA_ROOT / "logs"
METRICS_FOLDER = DATA_ROOT / "metrics"

FAULT_CASES = ["20231207", "20231221", "20240115", "20240124", "20240207", "20240215"]

MAX_SUSPECTS = 10

DATASET_HINTS = {
    "20231207": "One pod has CPU rising gradually (latent bug), causing latency spikes for some users.",
    "20231221": "Pod of microserviceB moved to different Node, causing load increase affecting microserviceA (Noisy Neighbor).",
    "20240115": "Container infected with malware, successfully logged in to other containers, becomes bot for DDoS.",
    "20240124": "Application froze and occupied one CPU. Server shows 25% CPU but actually one core maxed out.",
    "20240207": "Manifest deployed with incorrect resource limits. System runs initially, then hits limit.",
    "20240215": "Cryptojacking: Coin Miner installed, gradually affecting resources, increasing cloud usage fees."
}

RESULTS_FOLDER = Path("results")
RESULTS_FOLDER.mkdir(exist_ok=True)

# ============================================================================
# RANKED SUSPECT ENTRY
# ============================================================================

@dataclass
class RankedSuspect:
    """Represents a suspect with ranking information"""
    name: str
    detection_count: int = 0
    anomaly_count: int = 0
    confidence_score: float = 0.0
    detected_by_methods: List[str] = field(default_factory=list)

    def __lt__(self, other):
        """For sorting (highest score first)"""
        if self.confidence_score != other.confidence_score:
            return self.confidence_score > other.confidence_score
        return self.detection_count > other.detection_count

# ============================================================================
# STATE CLASS
# ============================================================================

@dataclass
class RCAState:
    """State for RCA workflow"""
    fault_case: str = ""
    hint: str = ""
    logs_path: str = ""
    metrics_path: str = ""
    has_logs: bool = True

    # Data storage
    kpi_data: Dict[str, pd.DataFrame] = field(default_factory=dict)
    pod_metrics: Dict[str, pd.DataFrame] = field(default_factory=dict)
    log_data: Dict[str, pd.DataFrame] = field(default_factory=dict)

    # Dataset insights
    pod_names: List[str] = field(default_factory=list)
    time_ranges: Dict[str, Any] = field(default_factory=dict)
    metric_info: Dict[str, List[str]] = field(default_factory=dict)

    # Gemini-selected context
    peer_metric_context: Dict[str, List[str]] = field(default_factory=dict)

    # Analysis
    kpi_insights: Dict[str, Any] = field(default_factory=dict)
    actions_to_perform: List[str] = field(default_factory=list)
    action_results: Dict[str, Any] = field(default_factory=dict)

    # Findings with ranking
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    suspect_tracking: Dict[str, 'RankedSuspect'] = field(default_factory=dict)
    suspects: List[str] = field(default_factory=list)
    suspect_details: List['RankedSuspect'] = field(default_factory=list)

    # Root Cause
    root_cause_found: bool = False
    affected_pod: Optional[str] = None
    bug_timestamp: Optional[str] = None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def load_csv_data(path: str) -> Dict[str, pd.DataFrame]:
    """Load all CSV files from a directory"""
    data = {}

    if not Path(path).exists():
        print(f"  ! Path does not exist: {path}")
        return data

    print(f"  Loading from {path}")

    csv_files = glob.glob(str(Path(path) / "*.csv"))
    print(f"  Found {len(csv_files)} CSV files")

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            file_name = Path(csv_file).stem

            first_col = df.columns[0]
            try:
                df[first_col] = pd.to_datetime(df[first_col])
            except:
                pass

            data[file_name] = df
            print(f"    ✓ {file_name} ({df.shape[0]} rows)")
        except Exception as e:
            print(f"    ✗ Error loading {Path(csv_file).stem}: {str(e)[:50]}")

    return data

def extract_pod_names(state: RCAState) -> List[str]:
    """Extract unique pod names from metrics"""
    pod_names = set()

    for df in state.pod_metrics.values():
        for col in df.columns[1:]:
            if col != 'total':
                pod_names.add(col)

    return sorted(list(pod_names))

def get_time_ranges(state: RCAState) -> Dict[str, Any]:
    """Get time ranges from data"""
    ranges = {}

    for name, df in {**state.kpi_data, **state.pod_metrics, **state.log_data}.items():
        first_col = df.columns[0]
        if pd.api.types.is_datetime64_any_dtype(df[first_col]):
            ranges[name] = {
                'start': str(df[first_col].min()),
                'end': str(df[first_col].max())
            }

    return ranges

def get_numeric_columns(state: RCAState) -> Dict[str, List[str]]:
    """Get numeric columns per metric file"""
    metric_info = {}

    for name, df in state.pod_metrics.items():
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_cols = [c for c in numeric_cols if c != 'total']
        metric_info[name] = numeric_cols

    return metric_info

def ask_gemini_for_context(state: RCAState) -> Dict[str, List[str]]:
    """Use Gemini to intelligently select peer-metric context groups"""

    if not HAS_GEMINI or not GEMINI_API_KEY:
        print("  ! No GEMINI_API_KEY or google.generativeai not available, using default grouping")
        return state.metric_info

    try:
        genai.configure(api_key=GEMINI_API_KEY)

        # Build prompt
        metric_summary = json.dumps(state.metric_info, indent=2)

        prompt = f"""Given these microservice pod-level metrics:

{metric_summary}

Each key is a metric file name, and values are numeric column names.

For ROOT CAUSE ANALYSIS, group these metrics by SEMANTIC RELATIONSHIP.
Which metrics should be compared together as peer context?

Return ONLY valid JSON (no markdown, no explanation):
{{
  "group_name_1": ["metric1", "metric2"],
  "group_name_2": ["metric3", "metric4"]
}}

Example:
{{
  "cpu_metrics": ["pod_level_data_cpu_usage"],
  "memory_metrics": ["pod_level_data_memory_usage"],
  "network_metrics": ["pod_level_data_rate_received_packets", "pod_level_data_rate_transmitted_packets"],
  "storage_metrics": ["pod_level_data_rate_storage_iops"]
}}
"""

        model = genai.GenerativeModel(GEMINI_MODEL)
        response = model.generate_content(prompt)

        response_text = response.text

        # Extract JSON
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}') + 1

        if start_idx >= 0 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx]
            peer_context = json.loads(json_str)
            print(f"  ✓ Gemini selected {len(peer_context)} peer-metric context groups:")
            for group_name, metrics in peer_context.items():
                print(f"    - {group_name}: {len(metrics)} metrics")
            return peer_context
        else:
            print(f"  ! Could not parse Gemini response, using default grouping")
            return state.metric_info

    except Exception as e:
        print(f"  ! Gemini call failed: {str(e)[:80]}, using default grouping")
        return state.metric_info

def track_suspect(state: RCAState, suspect_name: str, detection_method: str, anomaly_count: int = 1):
    """Track a suspect with its detection information"""
    if suspect_name not in state.suspect_tracking:
        state.suspect_tracking[suspect_name] = RankedSuspect(name=suspect_name)

    suspect = state.suspect_tracking[suspect_name]
    if detection_method not in suspect.detected_by_methods:
        suspect.detection_count += 1
        suspect.detected_by_methods.append(detection_method)
    suspect.anomaly_count += anomaly_count

# ============================================================================
# WORKFLOW NODES
# ============================================================================

def node_load_data(state: RCAState) -> RCAState:
    """Load all data"""
    print(f"\n[1/9] LOAD_DATA: {state.fault_case}")

    state.kpi_data = load_csv_data(state.metrics_path)
    state.pod_metrics = {k: v for k, v in load_csv_data(state.metrics_path).items() 
                         if k.startswith('pod_level_data')}
    state.log_data = load_csv_data(state.logs_path)

    if len(state.log_data) == 0:
        state.has_logs = False
        print(f"  ⚠ No log data available (metrics-only case)")
    else:
        state.has_logs = True

    print(f"  ✓ Loaded: {len(state.kpi_data)} KPI, {len(state.pod_metrics)} pod metrics, {len(state.log_data)} logs")

    return state

def node_learn_dataset_structure(state: RCAState) -> RCAState:
    """Learn dataset structure"""
    print(f"\n[2/9] LEARN_STRUCTURE")

    state.pod_names = extract_pod_names(state)
    state.time_ranges = get_time_ranges(state)
    state.metric_info = get_numeric_columns(state)

    print(f"  ✓ Pods identified: {len(state.pod_names)} pods")
    print(f"    Pods: {', '.join(state.pod_names[:3])}{'...' if len(state.pod_names) > 3 else ''}")
    print(f"  ✓ Metric files: {len(state.metric_info)} groups")

    if state.has_logs:
        print(f"  ✓ Log data available")
    else:
        print(f"  ⚠ Metrics-only case (no logs)")

    return state

def node_analyze_kpi_files(state: RCAState) -> RCAState:
    """Analyze KPI files"""
    print(f"\n[3/9] ANALYZE_KPI")

    for name, df in state.kpi_data.items():
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        stats = {
            'file': name,
            'rows': len(df),
            'numeric_cols': list(numeric_cols)
        }

        state.kpi_insights[name] = stats
        print(f"  ✓ {name}: {len(numeric_cols)} numeric columns")

    return state

def node_ask_gemini_for_context(state: RCAState) -> RCAState:
    """Ask Gemini to select peer-metric context groups"""
    print(f"\n[4/9] ASK_GEMINI_FOR_CONTEXT")

    state.peer_metric_context = ask_gemini_for_context(state)

    return state

def node_create_actions(state: RCAState) -> RCAState:
    """Create analysis actions"""
    print(f"\n[5/9] CREATE_ACTIONS")

    state.actions_to_perform = [
        "detect_contextual_spikes",
        "detect_divergent_pods",
        "detect_peer_metric_anomalies",
        "detect_correlation_breaks",
        "detect_gradual_degradation",
        "analyze_temporal_patterns"
    ]

    if state.has_logs:
        state.actions_to_perform.append("analyze_log_patterns")
        print(f"  ✓ Created {len(state.actions_to_perform)} actions (with log analysis)")
    else:
        print(f"  ✓ Created {len(state.actions_to_perform)} actions (metrics-only)")

    return state

def node_perform_action(state: RCAState) -> RCAState:
    """Perform all analysis actions"""
    print(f"\n[6/9] PERFORM_ACTION")

    for action in state.actions_to_perform:
        try:
            if action == "detect_contextual_spikes":
                result = detect_contextual_spikes(state)
            elif action == "detect_divergent_pods":
                result = detect_divergent_pods(state)
            elif action == "detect_peer_metric_anomalies":
                result = detect_peer_metric_anomalies(state)
            elif action == "detect_correlation_breaks":
                result = detect_correlation_breaks(state)
            elif action == "detect_gradual_degradation":
                result = detect_gradual_degradation(state)
            elif action == "analyze_temporal_patterns":
                result = analyze_temporal_patterns(state)
            elif action == "analyze_log_patterns":
                result = analyze_log_patterns(state)
            else:
                result = {'anomalies': [], 'suspects': [], 'timestamp': None}

            state.action_results[action] = result

            if result.get('suspects'):
                print(f"  ✓ {action}: {len(result.get('anomalies', []))} anomalies")
        except Exception as e:
            print(f"  ✗ {action} failed: {str(e)[:50]}")

    return state

def node_analyze_result(state: RCAState) -> RCAState:
    """Analyze and synthesize results with RANKING"""
    print(f"\n[7/9] ANALYZE_RESULT")

    total_anomalies = 0

    for action, result in state.action_results.items():
        if result.get('anomalies'):
            state.anomalies.extend(result['anomalies'])
            total_anomalies += len(result['anomalies'])

        if result.get('suspects'):
            for suspect in result['suspects']:
                anomaly_count = len(result.get('anomalies', []))
                track_suspect(state, suspect, action, anomaly_count)

    # Calculate confidence scores
    if total_anomalies > 0:
        for suspect_name, suspect_obj in state.suspect_tracking.items():
            suspect_obj.confidence_score = (suspect_obj.detection_count * suspect_obj.anomaly_count) / total_anomalies

    # Sort and limit
    sorted_suspects = sorted(state.suspect_tracking.values())
    state.suspect_details = sorted_suspects[:MAX_SUSPECTS]
    state.suspects = [s.name for s in state.suspect_details]

    print(f"  ✓ Found {len(state.anomalies)} anomalies, {len(state.suspect_tracking)} unique suspects")
    print(f"  ✓ Ranked top {len(state.suspects)} suspects")
    for i, suspect in enumerate(state.suspect_details[:3], 1):
        print(f"    {i}. {suspect.name} (score: {suspect.confidence_score:.3f})")

    return state

def node_check_root_cause(state: RCAState) -> RCAState:
    """Determine if root cause is found"""
    print(f"\n[8/9] CHECK_ROOT_CAUSE")

    if state.suspects and state.anomalies:
        state.root_cause_found = True
        print(f"  ✓ Root cause indicators found!")
    else:
        state.root_cause_found = False
        print(f"  ✗ Insufficient evidence")

    return state

def node_summarize(state: RCAState) -> RCAState:
    """Identify affected pod and timestamp"""
    print(f"\n[9/9] SUMMARIZE")

    state.affected_pod = state.suspects[0] if state.suspects else "Unknown"
    state.bug_timestamp = "Unknown"

    if state.anomalies:
        first_anomaly = state.anomalies[0]
        if 'timestamp' in first_anomaly:
            state.bug_timestamp = str(first_anomaly['timestamp'])

    if state.affected_pod != "Unknown":
        for metric_name, df in state.pod_metrics.items():
            if state.affected_pod in df.columns:
                col_data = df[state.affected_pod]
                first_col = df.columns[0]

                if pd.api.types.is_datetime64_any_dtype(df[first_col]):
                    mean_val = col_data.mean()
                    std_val = col_data.std()

                    anomaly_mask = np.abs(col_data - mean_val) > 2.5 * std_val
                    if anomaly_mask.any():
                        anomaly_idx = anomaly_mask.idxmax()
                        state.bug_timestamp = str(df[first_col].iloc[anomaly_idx])
                        break

    print(f"  ✓ Affected Pod: {state.affected_pod}")
    print(f"  ✓ Bug Timestamp: {state.bug_timestamp}")

    return state

# ============================================================================
# ANOMALY DETECTION METHODS
# ============================================================================

def detect_contextual_spikes(state: RCAState) -> Dict[str, Any]:
    """Detect contextual spikes"""
    anomalies = []
    suspects = []
    timestamp = None

    for name, df in state.kpi_data.items():
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        first_col = df.columns[0]

        for col in numeric_cols:
            try:
                window = max(5, len(df) // 20)
                rolling_mean = df[col].rolling(window=window, center=True).mean()
                rolling_std = df[col].rolling(window=window, center=True).std()

                zscore = np.abs((df[col] - rolling_mean) / (rolling_std + 1e-8))
                anomaly_idx = (zscore > 2.5).idxmax() if (zscore > 2.5).any() else None

                if anomaly_idx:
                    anomalies.append({'type': 'contextual_spike', 'metric': col})
                    if 'pod' in col.lower():
                        suspects.append(col)
                    if not timestamp and pd.api.types.is_datetime64_any_dtype(df[first_col]):
                        timestamp = str(df[first_col].iloc[anomaly_idx])
            except:
                pass

    return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

def detect_divergent_pods(state: RCAState) -> Dict[str, Any]:
    """Detect divergent pods"""
    anomalies = []
    suspects = []
    timestamp = None

    for name, df in state.pod_metrics.items():
        pod_cols = [c for c in df.columns[1:] if c != 'total']
        first_col = df.columns[0]

        if len(pod_cols) < 2:
            continue

        numeric_df = df[pod_cols].select_dtypes(include=[np.number])

        for col in numeric_df.columns:
            try:
                mean_val = numeric_df[col].mean()
                other_mean = numeric_df.drop(columns=[col]).values.flatten().mean()

                if abs(mean_val - other_mean) > other_mean * 0.3:
                    anomalies.append({'type': 'divergent_pod', 'pod': col})
                    suspects.append(col)
                    if not timestamp and pd.api.types.is_datetime64_any_dtype(df[first_col]):
                        divergence_idx = (numeric_df[col] > other_mean * 1.3).idxmax()
                        if divergence_idx:
                            timestamp = str(df[first_col].iloc[divergence_idx])
            except:
                pass

    return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

def detect_peer_metric_anomalies(state: RCAState) -> Dict[str, Any]:
    """Detect anomalies using peer-metric context (Gemini-selected)"""
    anomalies = []
    suspects = []
    timestamp = None

    for context_group_name, related_metrics in state.peer_metric_context.items():
        group_data = []
        pod_names_in_group = []

        for metric_col in related_metrics:
            for df_name, df in state.pod_metrics.items():
                if metric_col in df.columns:
                    group_data.append(df[metric_col].values)
                    pod_names_in_group.append(metric_col)

        if len(group_data) < 2:
            continue

        group_matrix = np.column_stack(group_data)
        first_df = next(iter(state.pod_metrics.values()))
        first_col_name = first_df.columns[0]

        for i, metric_col in enumerate(pod_names_in_group):
            try:
                target_series = group_matrix[:, i]
                peer_series = np.delete(group_matrix, i, axis=1).mean(axis=1)

                target_mean = target_series.mean()
                peer_mean = peer_series.mean()
                peer_std = peer_series.std()

                zscore = abs((target_mean - peer_mean) / (peer_std + 1e-8))

                if zscore > 2.0:
                    anomalies.append({'type': 'peer_metric_divergence', 'metric': metric_col})
                    suspects.append(metric_col)

                    if not timestamp and pd.api.types.is_datetime64_any_dtype(first_df[first_col_name]):
                        max_idx = np.argmax(np.abs(target_series - peer_mean))
                        timestamp = str(first_df[first_col_name].iloc[max_idx])
            except:
                pass

    return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

def detect_correlation_breaks(state: RCAState) -> Dict[str, Any]:
    """Detect correlation breaks"""
    anomalies = []
    suspects = []

    for metric_name, metric_df in state.pod_metrics.items():
        for kpi_name, kpi_df in state.kpi_data.items():
            try:
                metric_cols = metric_df.select_dtypes(include=[np.number]).columns
                kpi_cols = kpi_df.select_dtypes(include=[np.number]).columns

                if len(metric_cols) == 0 or len(kpi_cols) == 0:
                    continue

                mid = len(metric_df) // 2
                corr_first = metric_df[metric_cols].iloc[:mid].corrwith(kpi_df[kpi_cols].iloc[:mid]).abs().mean()
                corr_second = metric_df[metric_cols].iloc[mid:].corrwith(kpi_df[kpi_cols].iloc[mid:]).abs().mean()

                if abs(corr_first - corr_second) > 0.3:
                    anomalies.append({'type': 'correlation_break'})
                    for col in metric_cols:
                        suspects.append(col)
            except:
                pass

    return {'anomalies': anomalies, 'suspects': list(set(suspects)), 'timestamp': None}

def detect_gradual_degradation(state: RCAState) -> Dict[str, Any]:
    """Detect gradual degradation"""
    anomalies = []
    suspects = []
    timestamp = None

    for name, df in state.kpi_data.items():
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        first_col = df.columns[0]

        for col in numeric_cols:
            try:
                x = np.arange(len(df))
                y = df[col].values

                valid_mask = ~np.isnan(y)
                x_valid = x[valid_mask]
                y_valid = y[valid_mask]

                if len(x_valid) > 10:
                    z = np.polyfit(x_valid, y_valid, 1)
                    trend = z[0]

                    if abs(trend) > np.std(y_valid) * 0.01:
                        anomalies.append({'type': 'gradual_degradation'})
                        if not timestamp and pd.api.types.is_datetime64_any_dtype(df[first_col]):
                            timestamp = str(df[first_col].iloc[0])
            except:
                pass

    return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

def analyze_temporal_patterns(state: RCAState) -> Dict[str, Any]:
    """Analyze temporal patterns"""
    anomalies = []
    suspects = []
    timestamp = None

    for name, df in state.kpi_data.items():
        try:
            first_col = df.columns[0]
            numeric_cols = df.select_dtypes(include=[np.number]).columns

            if pd.api.types.is_datetime64_any_dtype(df[first_col]):
                for col in numeric_cols:
                    diff = df[col].diff().abs()
                    threshold = diff.mean() + 2 * diff.std()
                    jumps = diff > threshold

                    if jumps.sum() > 0:
                        jump_idx = jumps.idxmax()
                        anomalies.append({'type': 'temporal_jump'})
                        if not timestamp:
                            timestamp = str(df[first_col].iloc[jump_idx])
        except:
            pass

    return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

def analyze_log_patterns(state: RCAState) -> Dict[str, Any]:
    """Analyze log patterns"""
    anomalies = []
    suspects = []
    timestamp = None

    if not state.has_logs:
        return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

    try:
        for name, df in state.log_data.items():
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            first_col = df.columns[0]

            for col in numeric_cols:
                try:
                    mean_val = df[col].mean()
                    std_val = df[col].std()

                    spikes = df[col] > (mean_val + 2 * std_val)
                    if spikes.sum() > 0:
                        spike_idx = spikes.idxmax()
                        anomalies.append({'type': 'log_spike'})
                        if 'pod' in col.lower():
                            suspects.append(col)
                        if not timestamp and pd.api.types.is_datetime64_any_dtype(df[first_col]):
                            timestamp = str(df[first_col].iloc[spike_idx])
                except:
                    pass
    except:
        pass

    return {'anomalies': anomalies, 'suspects': suspects, 'timestamp': timestamp}

# ============================================================================
# RESULTS & MERMAID
# ============================================================================

def save_mermaid_diagram():
    """Save mermaid diagram"""
    print(f"\n[DIAGRAM] Generating workflow diagram...")

    mermaid_file = RESULTS_FOLDER / "workflow_diagram.mmd"
    with open(mermaid_file, 'w') as f:
        f.write(MERMAID_DIAGRAM)

    print(f"  ✓ Mermaid saved: {mermaid_file}")

def save_results(state: RCAState):
    """Save results"""
    results_file = RESULTS_FOLDER / f"{state.fault_case}_root_cause.txt"

    print(f"\n[OUTPUT] Saving results...")

    content = []
    content.append("="*80)
    content.append(f"ROOT CAUSE ANALYSIS - {state.fault_case}")
    content.append("="*80)
    content.append("")

    content.append(f"AFFECTED POD: {state.affected_pod}")
    content.append(f"BUG TIMESTAMP: {state.bug_timestamp}")
    content.append("")

    if not state.has_logs:
        content.append("DATA SOURCE: Metrics only (no logs)")
        content.append("")

    if state.root_cause_found:
        content.append("ROOT CAUSE FOUND: YES")
        content.append(f"ANOMALIES: {len(state.anomalies)}")
        content.append("")
        content.append(f"SUSPECT PODS - RANKED ({len(state.suspect_tracking)} total, top {len(state.suspects)})")
        content.append("-" * 80)

        for rank, suspect in enumerate(state.suspect_details, 1):
            detected_by = ", ".join(suspect.detected_by_methods)
            content.append(f"{rank}. {suspect.name}")
            content.append(f"   Score: {suspect.confidence_score:.3f} | Methods: {suspect.detection_count} | Anomalies: {suspect.anomaly_count}")
            content.append(f"   Detected by: {detected_by}")
            content.append("")
    else:
        content.append("ROOT CAUSE FOUND: NO")

    content.append("="*80)

    result_text = "\n".join(content)
    with open(results_file, 'w') as f:
        f.write(result_text)

    print(f"  ✓ Results: {results_file}")
    print("\n" + result_text)

# ============================================================================
# MAIN
# ============================================================================

def run_workflow(fault_case: str, hint: str, logs_path: str, metrics_path: str) -> RCAState:
    """Execute workflow"""
    print("\n" + "="*80)
    print(f"RCA WORKFLOW - {fault_case}")
    print("="*80)

    state = RCAState(
        fault_case=fault_case,
        hint=hint,
        logs_path=logs_path,
        metrics_path=metrics_path
    )

    state = node_load_data(state)
    state = node_learn_dataset_structure(state)
    state = node_analyze_kpi_files(state)
    state = node_ask_gemini_for_context(state)
    state = node_create_actions(state)
    state = node_perform_action(state)
    state = node_analyze_result(state)
    state = node_check_root_cause(state)
    state = node_summarize(state)

    return state

def main():
    """Main execution"""

    print("\n" + "="*80)
    print(f"RCA v6 - Peer-Metric Context + Gemini (FIXED)")
    print("="*80)

    if HAS_GEMINI and GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        print("✓ Gemini API configured")
    else:
        print("! Gemini not available - using default grouping")

    save_mermaid_diagram()

    processed = 0
    skipped = 0

    for i, fault_case in enumerate(FAULT_CASES, 1):
        print(f"\n[{i}/{len(FAULT_CASES)}] {fault_case}")

        logs_path = str(LOGS_FOLDER / fault_case)
        metrics_path = str(METRICS_FOLDER / fault_case)

        if not Path(metrics_path).exists():
            print(f"  ✗ No metrics data")
            skipped += 1
            continue

        try:
            state = run_workflow(fault_case, "", logs_path, metrics_path)
            save_results(state)
            processed += 1
        except Exception as e:
            print(f"  ✗ Error: {str(e)[:80]}")
            skipped += 1

    print(f"\n{'='*80}")
    print(f"✓ Processed: {processed} | Skipped: {skipped}")
    print(f"✓ Results: {RESULTS_FOLDER}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
